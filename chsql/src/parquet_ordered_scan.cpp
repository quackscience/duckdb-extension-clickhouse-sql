#include <duckdb.hpp>
#include "duckdb/common/exception.hpp"
#include <parquet_reader.hpp>
#include "chsql_extension.hpp"
#include <duckdb/common/multi_file_list.hpp>
#include "chsql_parquet_types.h"

namespace duckdb {

	struct ReturnColumn {
		string name;
		LogicalType type;
	};

	struct ReaderSet {
		unique_ptr<ParquetReader> reader;
		vector<ReturnColumn> returnColumns;
		int64_t orderByIdx;
		unique_ptr<DataChunk> chunk;
		unique_ptr<ParquetReaderScanState> scanState;
		vector<int64_t> columnMap;
		idx_t result_idx;
		bool haveAbsentColumns;
		void populateColumnInfo(const vector<ReturnColumn>& returnCols, const string& order_by_column) {
			this->returnColumns = returnCols;
			columnMap.clear();
			haveAbsentColumns = false;
			for (auto it = returnCols.begin(); it!= returnCols.end(); ++it) {
				auto schema_column = find_if(
					reader->metadata->metadata->schema.begin(),
					reader->metadata->metadata->schema.end(),
					[&](const SchemaElement& column) { return column.name == it->name; });
				if (schema_column == reader->metadata->metadata->schema.end()) {
                    columnMap.push_back(-1);
					haveAbsentColumns = true;
					continue;
                }
				columnMap.push_back(schema_column - reader->metadata->metadata->schema.begin() - 1);
				reader->reader_data.column_ids.push_back(
					schema_column - reader->metadata->metadata->schema.begin() - 1);
				reader->reader_data.column_mapping.push_back(
					it - returnCols.begin());
			}
			auto order_by_column_it = find_if(
				reader->metadata->metadata->schema.begin(),
                reader->metadata->metadata->schema.end(),
                [&](const SchemaElement& column) { return column.name == order_by_column; });
            if (order_by_column_it == reader->metadata->metadata->schema.end()) {
	            orderByIdx = -1;
            } else {
                orderByIdx = order_by_column_it - reader->metadata->metadata->schema.begin() - 1;
            }
		}
		void Scan(ClientContext& ctx) {
			chunk->Reset();
			reader->Scan(*scanState, *chunk);
			if (!haveAbsentColumns || chunk->size() == 0) {
				return;
			}
			for (auto it = columnMap.begin(); it!=columnMap.end(); ++it) {
				if (*it != -1) {
					continue;
				}
				chunk->data[it - columnMap.begin()].Initialize(false, chunk->size());
				for (idx_t j = 0; j < chunk->size(); j++) {
					chunk->data[it - columnMap.begin()].SetValue(j, Value());
				}
			}
		}
	};

	struct OrderedReadFunctionData : FunctionData {
		string orderBy;
		vector<string> files;
		vector<ReturnColumn> returnCols;
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

	bool lt(const Value &left, const Value &right) {
		return left.IsNull() || (!right.IsNull() && left < right);
	}

	bool le(const Value &left, const Value &right) {
		return left.IsNull() || (!right.IsNull() && left <= right);
	}

	struct OrderedReadLocalState: LocalTableFunctionState {
		vector<unique_ptr<ReaderSet>> sets;
		vector<idx_t> winner_group;
		void RecalculateWinnerGroup() {
			winner_group.clear();
			if (sets.empty()) {
				return;
			}
			idx_t winner_idx = 0;
			auto first_unordered = std::find_if(sets.begin(), sets.end(),
				[&](const unique_ptr<ReaderSet> &s) { return s->orderByIdx == -1; });
			if (first_unordered != sets.end()) {
				winner_group.push_back(first_unordered - sets.begin());
				return;
			}
			for (idx_t i = 1; i < sets.size(); i++) {
				const auto &s = sets[i];
				const auto &w = sets[winner_idx];
				if (lt(s->chunk->GetValue(s->orderByIdx, s->result_idx),
					w->chunk->GetValue(w->orderByIdx, w->result_idx))) {
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
				if (le(sFirst, wLast)) {
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

	static vector<ReturnColumn> GetColumnsFromParquetSchemas(const vector<unique_ptr<ReaderSet>>& sets) {
		vector<ReturnColumn> result;
        for (auto &set : sets) {
            const auto &schema = set->reader->metadata->metadata->schema;
            for (auto it = schema.begin(); it != schema.end(); ++it) {
            	if (it->num_children > 0) {
            		continue;
            	}
            	auto type = ParquetTypesManager::get_logical_type(schema, it - schema.begin());
            	auto existing_col = std::find_if(result.begin(), result.end(),
            		[it](const ReturnColumn &c) { return c.name == it->name; });
            	if (existing_col == result.end()) {
            		result.push_back(ReturnColumn{it->name, type});
            		continue;
            	}
            	if (existing_col->type != type) {
            		throw std::runtime_error("the files have incompatible schema");
            	}
            }
        }
        return result;
	}


	static void OpenParquetFiles(ClientContext &context, const vector<string>& fileNames,
		vector<unique_ptr<ReaderSet>>& res) {
		for (auto & file : fileNames) {
			auto set = make_uniq<ReaderSet>();
			ParquetOptions po;
			po.binary_as_string = true;
			set->reader = make_uniq<ParquetReader>(context, file, po, nullptr);
			res.push_back(move(set));
		}
	}

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
		while (fileList.Scan(it, filename)) {
			res->files.push_back(filename);
		}
		if (res->files.empty()) {
		    throw InvalidInputException("No files matched the provided pattern.");
		}

		vector<unique_ptr<ReaderSet>> sets;
		OpenParquetFiles(context, res->files, sets);

		res->returnCols = GetColumnsFromParquetSchemas(sets);
		std::transform(res->returnCols.begin(), res->returnCols.end(), std::back_inserter(names),
			[](const ReturnColumn &c) { return c.name; });
		std::transform(res->returnCols.begin(), res->returnCols.end(), std::back_inserter(return_types),
			[](const ReturnColumn &c) { return c.type; });

		res->orderBy = input.inputs[1].GetValue<string>();
		return std::move(res);
	}

	static unique_ptr<LocalTableFunctionState>
	ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
		auto res = make_uniq<OrderedReadLocalState>();
		const auto &bindData = input.bind_data->Cast<OrderedReadFunctionData>();
		OpenParquetFiles(context.client, bindData.files, res->sets);

		for (auto &set : res->sets) {
			set->populateColumnInfo(bindData.returnCols, bindData.orderBy);
			set->scanState = make_uniq<ParquetReaderScanState>();
			vector<idx_t> rgs(set->reader->metadata->metadata->row_groups.size(), 0);
			for (idx_t i = 0; i < rgs.size(); i++) {
				rgs[i] = i;
			}
			set->reader->InitializeScan(context.client, *set->scanState, rgs);
			set->chunk = make_uniq<DataChunk>();
			set->result_idx = 0;
			auto ltypes = vector<LogicalType>();
			std::transform(bindData.returnCols.begin(), bindData.returnCols.end(), std::back_inserter(ltypes),
				[](const ReturnColumn &c) { return c.type; });
			set->chunk->Initialize(context.client, ltypes);
			set->Scan(context.client);
		}
		res->RecalculateWinnerGroup();
		return std::move(res);
	}

	static void ParquetOrderedScanImplementation(
		ClientContext &context, duckdb::TableFunctionInput &data_p,DataChunk &output) {
		auto &loc_state = data_p.local_state->Cast<OrderedReadLocalState>();
		const auto &cols = data_p.bind_data->Cast<OrderedReadFunctionData>().returnCols;
		bool toRecalc = false;
		for (int i = loc_state.sets.size() - 1; i >= 0 ; i--) {
			if (loc_state.sets[i]->result_idx >= loc_state.sets[i]->chunk->size()) {
				auto &set = loc_state.sets[i];
				set->chunk->Reset();
				loc_state.sets[i]->Scan(context);
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
									(*winnerSet)->result_idx);
			for (int k = 1; k < loc_state.winner_group.size(); k++) {
				const auto i = loc_state.winner_group[k];
				const auto &set = loc_state.sets[i];
				const Value &val = set->chunk->GetValue(set->orderByIdx, set->result_idx);
				if (lt(val, winner_val)) {
					winnerSet = &loc_state.sets[i];
					winner_val = (*winnerSet)->chunk->GetValue(set->orderByIdx, set->result_idx);
				}
			}
			for (int i = 0; i < cols.size(); i++) {
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
