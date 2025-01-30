#include <duckdb.hpp>
#include "duckdb/common/exception.hpp"
#include <parquet_reader.hpp>
#include "chsql_extension.hpp"
#include <duckdb/common/multi_file_list.hpp>

namespace duckdb {

	struct ReaderSet {
		unique_ptr<ParquetReader> reader;
		idx_t orderByIdx;
		unique_ptr<DataChunk> chunk;
		unique_ptr<ParquetReaderScanState> scanState;
		vector<idx_t> columnMap;
		idx_t result_idx;
	};

	struct OrderedReadFunctionData : FunctionData {
		string orderBy;
		vector<unique_ptr<ReaderSet>> sets;
		vector<string> files;
		vector<LogicalType> returnTypes;
		vector<string> names;
		unique_ptr<FunctionData> Copy() const override {
			throw std::runtime_error("not implemented");
		}
		static bool EqualStrArrays(const vector<string> &a, const vector<string> &b) {
			if (a.size() != b.size()) {
				return false;
			}
			for (int i = 0; i < a.size(); i++) {
				if (a[i] != b[i]) {
					return false;
				}
			}
			return true;
		}
		bool Equals(const FunctionData &other) const override {
			const auto &o = other.Cast<OrderedReadFunctionData>();
			if (!EqualStrArrays(o.files, files)) {
				return false;
			}
			return this->orderBy ==  o.orderBy;
		};
	};



	struct  OrderedReadLocalState: LocalTableFunctionState {
		vector<unique_ptr<ReaderSet>> sets;
		vector<idx_t> winner_group;
		void RecalculateWinnerGroup() {
			winner_group.clear();
			if (sets.empty()) {
				return;
			}
			idx_t winner_idx = 0;
			for (idx_t i = 1; i < sets.size(); i++) {
				const auto &s = sets[i];
				const auto &w = sets[winner_idx];
				if (s->chunk->GetValue(s->orderByIdx, s->result_idx) <
					w->chunk->GetValue(w->orderByIdx, w->result_idx)) {
					winner_idx = i;
					}
			}
			winner_group.push_back(winner_idx);
			auto &w = sets[winner_idx];
			const auto &wLast = w->chunk->GetValue(w->orderByIdx, w->chunk->size()-1);
			for (idx_t i = 0; i < sets.size(); i++) {
				if (i == winner_idx)  continue;
				auto &s = sets[i];
				const auto &sFirst = s->chunk->GetValue(s->orderByIdx, s->result_idx);
				if (sFirst <= wLast) {
					winner_group.push_back(i);
				}
			}
		}
		void RemoveSetGracefully(const idx_t idx) {
			if (idx != sets.size() - 1) {
				sets[idx].reset(sets[sets.size() - 1].release());
			}
			sets.pop_back();
		}
	};


	static unique_ptr<FunctionData> OrderedParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
														vector<LogicalType> &return_types, vector<string> &names) {
		Connection conn(*context.db);
		auto res = make_uniq<OrderedReadFunctionData>();
		auto files = ListValue::GetChildren(input.inputs[0]);
		vector<string> fileNames;
		for (auto & file : files) {
			fileNames.push_back(file.ToString());
		}
		GlobMultiFileList fileList(context, fileNames, FileGlobOptions::ALLOW_EMPTY);
		string filename;
		MultiFileListScanData it;
		fileList.InitializeScan(it);
		vector<string> unglobbedFileList;
		while (fileList.Scan(it, filename)) {
			unglobbedFileList.push_back(filename);
		}
		if (unglobbedFileList.empty()) {
		    throw duckdb::InvalidInputException("No files matched the provided pattern.");
		}

		res->orderBy = input.inputs[1].GetValue<string>();
		for (auto & file : unglobbedFileList) {
			auto set = make_uniq<ReaderSet>();
			res->files.push_back(file);
			ParquetOptions po;
			po.binary_as_string = true;
			ParquetReader reader(context, file, po, nullptr);
			set->columnMap = vector<idx_t>();
			for (auto &el : reader.metadata->metadata->schema) {
				if (el.num_children != 0) {
					continue;
				}
				auto name_it = std::find(names.begin(), names.end(), el.name);
				auto return_type = LogicalType::ANY;
				switch (el.type) {
					case Type::INT32:
						return_type = LogicalType::INTEGER;
					break;
					case Type::INT64:
						return_type = LogicalType::BIGINT;
					break;
					case Type::DOUBLE:
						return_type = LogicalType::DOUBLE;
					break;
					case Type::FLOAT:
						return_type = LogicalType::FLOAT;
					break;
					case Type::BYTE_ARRAY:
						return_type = LogicalType::VARCHAR;
					break;
					case Type::FIXED_LEN_BYTE_ARRAY:
						return_type = LogicalType::VARCHAR;
					break;
					case Type::BOOLEAN:
						return_type = LogicalType::TINYINT;
					break;
					case Type::TIMESTAMP:
				            return_type = LogicalType::TIMESTAMP_NS; 
				        break;
					case Type::DATE:
				            return_type = LogicalType::DATE;
				        break;
					default:
						break;;
				}
				set->columnMap.push_back(name_it - names.begin());
				if (el.name == res->orderBy) {
					set->orderByIdx = name_it - names.begin();
				}
				if (name_it != names.end()) {
					if (return_types[name_it - names.begin()] != return_type) {
						throw std::runtime_error("incompatible schema");
					}
					continue;
				}
				return_types.push_back(return_type);
				names.push_back(el.name);
			}
			res->sets.push_back(std::move(set));
		}
		res->returnTypes = return_types;
		res->names = names;
		return std::move(res);
	}

	static unique_ptr<LocalTableFunctionState>
	ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
		auto res = make_uniq<OrderedReadLocalState>();
		const auto &bindData = input.bind_data->Cast<OrderedReadFunctionData>();
		ParquetOptions po;
		po.binary_as_string = true;
		for (int i = 0; i < bindData.files.size(); i++) {
			auto set = make_uniq<ReaderSet>();
			set->reader = make_uniq<ParquetReader>(context.client, bindData.files[i], po, nullptr);
			set->scanState = make_uniq<ParquetReaderScanState>();
			int j = 0;
			for (auto &el : set->reader->metadata->metadata->schema) {
				if (el.num_children != 0) {
					continue;
				}
				set->reader->reader_data.column_ids.push_back(j);
				j++;
			}
			set->columnMap = bindData.sets[i]->columnMap;
			set->reader->reader_data.column_mapping = set->columnMap;
			vector<idx_t> rgs(set->reader->metadata->metadata->row_groups.size(), 0);
			for (idx_t i = 0; i < rgs.size(); i++) {
				rgs[i] = i;
			}
			set->reader->InitializeScan(context.client, *set->scanState, rgs);
			set->chunk = make_uniq<DataChunk>();

			set->orderByIdx = bindData.sets[i]->orderByIdx;
			set->result_idx = 0;
			auto ltypes = vector<LogicalType>();
			for (const auto idx : set->columnMap) {
				ltypes.push_back(bindData.returnTypes[idx]);
			}
			set->chunk->Initialize(context.client, ltypes);
			set->reader->Scan(*set->scanState, *set->chunk);
			res->sets.push_back(std::move(set));
		}
		res->RecalculateWinnerGroup();
		return std::move(res);
	}

	static void ParquetOrderedScanImplementation(
		ClientContext &context, duckdb::TableFunctionInput &data_p,DataChunk &output) {
		auto &loc_state = data_p.local_state->Cast<OrderedReadLocalState>();
		const auto &fieldNames = data_p.bind_data->Cast<OrderedReadFunctionData>().names;
		const auto &returnTypes = data_p.bind_data->Cast<OrderedReadFunctionData>().returnTypes;
		bool toRecalc = false;
		for (int i = loc_state.sets.size() - 1; i >= 0 ; i--) {
			if (loc_state.sets[i]->result_idx >= loc_state.sets[i]->chunk->size()) {
				auto &set = loc_state.sets[i];
				set->chunk->Reset();
				loc_state.sets[i]->reader->Scan(
					*loc_state.sets[i]->scanState,
					*loc_state.sets[i]->chunk);
				loc_state.sets[i]->result_idx = 0;

				if (loc_state.sets[i]->chunk->size() == 0) {
					loc_state.RemoveSetGracefully(i);
				}
				toRecalc = true;
			}
		}
		if (loc_state.sets.empty()) {
			return;
		}
		if (toRecalc) {
			loc_state.RecalculateWinnerGroup();
		}
		int cap = 1024;
		output.Reset();
		output.SetCapacity(cap);
		idx_t j = 0;
		if (loc_state.winner_group.size() == 1) {
			auto &set = loc_state.sets[loc_state.winner_group[0]];
			set->chunk->Slice(set->result_idx, set->chunk->size() - set->result_idx);
			output.Append(*set->chunk, true);
			output.SetCardinality(set->chunk->size());
			set->result_idx = set->chunk->size();
			return;
		}
		while(true) {
			auto winnerSet = &loc_state.sets[loc_state.winner_group[0]];
			Value winner_val = (*winnerSet)->chunk->GetValue(
									(*winnerSet)->orderByIdx,
									(*winnerSet)->result_idx
									);
			for (int k = 1; k < loc_state.winner_group.size(); k++) {
				const auto i = loc_state.winner_group[k];
				const auto &set = loc_state.sets[i];
				const Value &val = set->chunk->GetValue(set->orderByIdx, set->result_idx);
				if (val < winner_val) {
					winnerSet = &loc_state.sets[i];
					winner_val = (*winnerSet)->chunk->GetValue(set->orderByIdx, set->result_idx);
				}
			}
			for (int i = 0; i < fieldNames.size(); i++) {
				const auto &val = (*winnerSet)->chunk->GetValue(i,(*winnerSet)->result_idx);
				output.SetValue(i, j, val);
			}
			j++;
			(*winnerSet)->result_idx++;
			if ((*winnerSet)->result_idx >= (*winnerSet)->chunk->size() || j >= 2048) {
				output.SetCardinality(j);
				return;
			}
			if (j >= cap) {
				cap *= 2;
				output.SetCapacity(cap);
			}
		}
	}

	TableFunction ReadParquetOrderedFunction() {
		TableFunction tf = duckdb::TableFunction(
			"read_parquet_mergetree",
			{LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
			ParquetOrderedScanImplementation,
			OrderedParquetScanBind,
			nullptr,
			ParquetScanInitLocal
			);
		return tf;
	}
}
