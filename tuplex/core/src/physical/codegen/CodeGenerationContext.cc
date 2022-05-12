//
// Created by leonhards on 3/23/22.
//

#include <physical/codegen/CodeGenerationContext.h>

namespace tuplex {
    namespace codegen {
        std::string CodeGenerationContext::toJSON() const {
            using namespace nlohmann;
            auto &logger = Logger::instance().logger("codegen");


            try {
                json root;

                // global settings, encode them!
                root["sharedObjectPropagation"] = sharedObjectPropagation;
                root["retypeUsingOptimizedInputSchema"] = nullValueOptimization;
                root["constantFoldingOptimization"] = constantFoldingOptimization;
                root["isRootStage"] = isRootStage;
                root["generateParser"] = generateParser;
                root["normalCaseThreshold"] = normalCaseThreshold;
                root["outputMode"] = outputMode;
                root["outputFileFormat"] = outputFileFormat;
                root["outputNodeID"] = outputNodeID;
                root["outputSchema"] = outputSchema.getRowType().desc();
                root["fileOutputParameters"] = fileOutputParameters;
                root["outputLimit"] = outputLimit;
                root["hashColKeys"] = hashColKeys;
                if(python::Type::UNKNOWN != hashKeyType && hashKeyType.hash() > 0) // HACK
                    root["hashKeyType"] = hashKeyType.desc();
                root["hashSaveOthers"] = hashSaveOthers;
                root["hashAggregate"] = hashAggregate;
                root["inputMode"] = inputMode;
                root["inputFileFormat"] = inputFileFormat;
                root["inputNodeID"] = inputNodeID;
                root["fileInputParameters"] = fileInputParameters;

                // encode codepath contexts as well!
                if(fastPathContext.valid())
                    root["fastPathContext"] = fastPathContext.to_json();
                if(slowPathContext.valid())
                    root["slowPathContext"] = slowPathContext.to_json();

                return root.dump();
            } catch(nlohmann::json::type_error& e) {
                logger.error(std::string("exception constructing json: ") + e.what());
                return "{}";
            }
        }

        nlohmann::json encodeOperator(LogicalOperator* op) {
            nlohmann::json obj;

            if(op->type() == LogicalOperatorType::FILEINPUT)
                return dynamic_cast<FileInputOperator*>(op)->to_json();
            else if(op->type() == LogicalOperatorType::MAP)
                return dynamic_cast<MapOperator*>(op)->to_json();
            else {
                throw std::runtime_error("unsupported operator " + op->name() + " seen for encoding... HACK");
            }

            return obj;
        }

        nlohmann::json CodeGenerationContext::CodePathContext::to_json() const {
            nlohmann::json obj;
            obj["read"] = readSchema.getRowType().desc();
            obj["input"] = inputSchema.getRowType().desc();
            obj["output"] = outputSchema.getRowType().desc();

            obj["colsToRead"] = columnsToRead;

            // now the most annoying thing, encoding the operators
            auto ops = nlohmann::json::array();
            ops.push_back(encodeOperator(inputNode.get()));
            for(auto op : operators)
                ops.push_back(encodeOperator(op.get()));

            obj["operators"] = ops;
            return std::move(obj);
        }

        // decode now everything...
        CodeGenerationContext CodeGenerationContext::fromJSON(const std::string &json_str) {
            CodeGenerationContext ctx;
            using namespace nlohmann;
            auto &logger = Logger::instance().logger("codegen");

            try {
                // try to extract json
                auto root = json::parse(json_str);

                // global settings, encode them!
                ctx.sharedObjectPropagation = root["sharedObjectPropagation"].get<bool>();
                ctx.nullValueOptimization = root["retypeUsingOptimizedInputSchema"].get<bool>();// = retypeUsingOptimizedInputSchema;
                ctx.constantFoldingOptimization = root["constantFoldingOptimization"].get<bool>();// = retypeUsingOptimizedInputSchema;
                ctx.isRootStage = root["isRootStage"].get<bool>();// = isRootStage;
                ctx.generateParser = root["generateParser"].get<bool>();// = generateParser;
                ctx.normalCaseThreshold = root["normalCaseThreshold"].get<double>();// = normalCaseThreshold;
                ctx.outputMode = static_cast<EndPointMode>(root["outputMode"].get<int>());// = outputMode;
                ctx.outputFileFormat = static_cast<FileFormat>(root["outputFileFormat"].get<int>());// = outputFileFormat;
                ctx.outputNodeID = root["outputNodeID"].get<int>();// = outputNodeID;
                ctx.outputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(root["outputSchema"].get<std::string>()));// = outputSchema.getRowType().desc();
                ctx.fileOutputParameters = root["fileOutputParameters"].get<std::unordered_map<std::string, std::string>>();// = fileOutputParameters;
                ctx.outputLimit = root["outputLimit"].get<size_t>();// = outputLimit;
                ctx.hashColKeys = root["hashColKeys"].get<std::vector<size_t>>();// = hashColKeys;

                ctx.hashSaveOthers = root["hashSaveOthers"].get<bool>();// = hashSaveOthers;
                ctx.hashAggregate = root["hashAggregate"].get<bool>();// = hashAggregate;
                ctx.inputMode = static_cast<EndPointMode>(root["inputMode"].get<int>());// = inputMode;
                ctx.inputFileFormat = static_cast<FileFormat>(root["inputFileFormat"].get<int>());
                ctx.inputNodeID = root["inputNodeID"].get<int>();// = inputNodeID;
                ctx.fileInputParameters = root["fileInputParameters"].get<std::unordered_map<std::string, std::string>>();// = fileInputParameters;

                // if(python::Type::UNKNOWN != hashKeyType && hashKeyType.hash() > 0) // HACK
                //     root["hashKeyType"] = hashKeyType.desc();

                if(root.find("fastPathContext") != root.end()) {
                    // won't be true, skip
                    ctx.fastPathContext = CodeGenerationContext::CodePathContext::from_json(root["fastPathContext"]);
                }
                if(root.find("slowPathContext") != root.end()) {
                    ctx.slowPathContext = CodeGenerationContext::CodePathContext::from_json(root["slowPathContext"]);
                }
            } catch(const json::parse_error& jse) {
                logger.error(std::string("failed to decode: ") + jse.what());
            }

            return ctx;
        }

        CodeGenerationContext::CodePathContext CodeGenerationContext::CodePathContext::from_json(nlohmann::json obj) {
            CodeGenerationContext::CodePathContext ctx;

            // decode schemas
            ctx.readSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["read"].get<std::string>()));
            ctx.inputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["input"].get<std::string>()));
            ctx.outputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["output"].get<std::string>()));
            ctx.columnsToRead = obj["colsToRead"].get<std::vector<bool>>();

            // now the worst, decoding the operators...
            std::vector<std::shared_ptr<LogicalOperator>> operators;
            for(auto json_op : obj["operators"]) {
                // only map and csv supported
                auto name = json_op["name"].get<std::string>();
                if(name == "csv") {
                    operators.push_back(std::shared_ptr<LogicalOperator>(FileInputOperator::from_json(json_op)));
                } else if(name == "map") {
                    // map is easy, simply decode UDF and hook up with parent operator!
                    assert(!operators.empty());
                    auto columnNames = json_op["columnNames"].get<std::vector<std::string>>();
                    auto id = json_op["id"].get<int>();
                    auto code = json_op["udf"]["code"].get<std::string>();
                    // @TODO: avoid typing call?
                    // i.e. this will draw a sample too?
                    // or ok, because sample anyways need to get drawn??
                    UDF udf(code);

                    auto mop = new MapOperator(operators.back(), udf, columnNames);
                    mop->setID(id);
                    operators.push_back(std::shared_ptr<LogicalOperator>(mop));
                } else {
                    throw std::runtime_error("attempting to decode unknown op " + name);
                }
            }
            ctx.inputNode = operators.front();
            ctx.operators = std::vector<std::shared_ptr<LogicalOperator>>(operators.begin() + 1, operators.end());


//            obj["read"] = readSchema.getRowType().desc();
//            obj["input"] = inputSchema.getRowType().desc();
//            obj["output"] = outputSchema.getRowType().desc();
//
//            obj["colsToRead"] = columnsToRead;
//
//            // now the most annoying thing, encoding the operators
//            auto ops = nlohmann::json::array();
//            ops.push_back(encodeOperator(inputNode));
//            for(auto op : operators)
//                ops.push_back(encodeOperator(op));
//
//            obj["operators"] = ops;
//            return obj;

            return ctx;
        }
    }
}