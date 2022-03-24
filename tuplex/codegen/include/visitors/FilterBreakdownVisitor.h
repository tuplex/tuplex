//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by rahuly first first on 02/13/21                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FILTERBREAKDOWNVISITOR_H
#define TUPLEX_FILTERBREAKDOWNVISITOR_H

#include <ast/ASTNodes.h>
#include <visitors/IPrePostVisitor.h>
#include <codegen/IFailable.h>
#include <string>
#include <utility>
#include <vector>
#include <stack>

namespace tuplex {
    class FilterBreakdownVisitor : public IPrePostVisitor, public IFailable {
    private:
        struct Interval {
            union {
                double dMin;
                int64_t iMin;
            };
            union {
                double dMax;
                int64_t iMax;
            };

            // String Intervals
            std::string sMin; // minimum string value
            std::pair<bool, std::string> sMax; // maximum string value; boolean - whether it holds a value or not (false -> unbounded max)
            bool inclusiveMin; // whether the lower bound is inclusive
            bool inclusiveMax; // whether the upper bound is inclusive

            /*!
             * Custom comparator for our string max representation -> returns the lesser of the two input upper bound values
             * @param a
             * @param b
             * @return
             */
            static bool stringUpperBoundLessThan(const std::pair<bool, std::string> &a, const std::pair<bool, std::string> &b) {
                if(!a.first && !b.first) return false; // both unbounded -> equal
                if(!a.first) return false; // a unbounded, b not -> b is less
                if(!b.first) return true; // b unbounded, a not -> a is less
                return a.second < b.second;
            }

            python::Type type;
            bool empty;

            Interval() : empty(true) {}

            Interval(int64_t _imin, int64_t _imax) : iMin(_imin), iMax(_imax), empty(false), type(python::Type::I64), inclusiveMin(true), inclusiveMax(true) {}

            Interval(double _dmin, double _dmax) : dMin(_dmin), dMax(_dmax), empty(false), type(python::Type::F64), inclusiveMin(true), inclusiveMax(true) {}

            Interval(std::string _sMin, std::string _sMax, bool _includeMin, bool _includeMax) : sMin(std::move(_sMin)),
                                                                                                 sMax(true,
                                                                                                      std::move(_sMax)),
                                                                                                 empty(false),
                                                                                                 type(python::Type::STRING),
                                                                                                 inclusiveMin(_includeMin),
                                                                                                 inclusiveMax(_includeMax) {}

            Interval(std::string _sMin, bool _includeMin) : sMin(std::move(_sMin)), sMax(false, ""), empty(false),
                                                            type(python::Type::STRING), inclusiveMin(_includeMin), inclusiveMax(false) {}

            Interval(std::string _sMin, std::pair<bool, std::string> _sMax, bool _includeMin, bool _includeMax) :
                    sMin(std::move(_sMin)), sMax(std::move(_sMax)), empty(false), type(python::Type::STRING),
                    inclusiveMin(_includeMin), inclusiveMax(_includeMax) {}

            static Interval fullDoubleRange() {
                return Interval(std::numeric_limits<double>::min(), std::numeric_limits<double>::max());
            }

            static Interval fullIntegerRange() {
                return Interval(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
            }

            static Interval fullStringRange() {
                return Interval("", true);
            }

            Interval intersectWith(const Interval &I) const {
                if((I.type == python::Type::I64 || I.type == python::Type::F64) && (type == python::Type::STRING)) throw std::runtime_error("Invalid intersection called!");
                if((type == python::Type::I64 || type == python::Type::F64) && (I.type == python::Type::STRING)) throw std::runtime_error("Invalid intersection called!");

                // https://stackoverflow.com/questions/20023482/c-finding-intersection-of-two-ranges
                // say for the range [l1, r1], [l2, r2] intersection between them can be calculated as:
                //
                // if ((r1 < l2) ||  (r2 < l1)) then no intersection exits.
                // else l = max(l1, l2) and r = min(r1, r2)
                if (I.type == python::Type::I64) {
                    int64_t low = iMin;
                    int64_t high = iMax;
                    if (type == python::Type::F64) {
                        low = floor(dMin);
                        high = ceil(dMax);
                    }
                    assert(low <= high);

                    if (high < I.iMin || I.iMax < low) {
                        // no intersect, empty interval
                        return {};
                    }

                    return {std::max(I.iMin, low), std::min(I.iMax, high)};
                } else if(I.type == python::Type::F64) {
                    double low = dMin;
                    double high = dMax;
                    if (type == python::Type::I64) {
                        low = iMin;
                        high = iMax;
                    }
                    assert(low <= high);

                    if (low < I.dMin || I.dMax < high) {
                        // no intersect, empty interval
                        return {};
                    }

                    return {std::max(I.dMin, low), std::min(I.dMax, high)};
                } else {
                    assert(type == python::Type::STRING);
                    // check if [I.sMin, I.sMax] is strictly below [sMin, sMax]
                    //  => if I.sMax exists and is less than sMin (or they are equal but one of them is exclusive)
                    if(I.sMax.first && (I.sMax.second < sMin)) return {};
                    if(I.sMax.first && (!I.inclusiveMax || !inclusiveMin) && (I.sMax.second == sMin)) return {};
                    // check if [I.sMin, I.sMax] is strictly above [sMin, sMax]
                    //  => sMax exists and is less than I.sMin (or they are equal but one of them is exclusive)
                    if(sMax.first && (sMax.second < I.sMin)) return {};
                    if(sMax.first && (!I.inclusiveMin || !inclusiveMax) && (sMax.second == I.sMin)) return {};

                    // combine the intervals
                    std::string new_low; bool includeLow;
                    if(I.sMin > sMin) { // take max of min
                        new_low = I.sMin; includeLow = I.inclusiveMin;
                    } else if(I.sMin == sMin) {
                        new_low = I.sMin; includeLow = I.inclusiveMin && inclusiveMin;
                    } else {
                        new_low = sMin; includeLow = inclusiveMin;
                    }

                    std::pair<bool, std::string> new_high; bool includeHigh;
                    if(stringUpperBoundLessThan(I.sMax, sMax)) { // take min of max
                        new_high = I.sMax; includeHigh = I.inclusiveMax;
                    } else if(stringUpperBoundLessThan(sMax, I.sMax)) {
                        new_high = sMax; includeHigh = inclusiveMax;
                    } else {
                        new_high = I.sMax; includeHigh = I.inclusiveMax && inclusiveMax;
                    }


                    return {new_low, new_high, includeLow, includeHigh};
                }
            }

            std::string createBooleanCondition(const std::string &lambdaVar) const {
                assert(!empty);
                if(type == python::Type::I64) {
                    if (iMin == iMax) return "(" + lambdaVar + " == " + std::to_string(iMin) + ")";
                    else
                        return "(" + std::to_string(iMin) + " <= " + lambdaVar + " <= " +
                               std::to_string(iMax) + ")";
                } else if(type == python::Type::F64) {
                    if (dMin == dMax) return "(" + lambdaVar + " == " + std::to_string(dMin) + ")";
                    else
                        return "(" + std::to_string(dMin) + " <= " + lambdaVar + " <= " +
                               std::to_string(dMax) + ")";
                } else {
                    std::string opMin = inclusiveMin ? "<=" : "<";
                    std::string opMax = inclusiveMax ? "<=" : "<";
                    std::string left = "'" + sMin + "'";
                    std::string right = "'" + sMax.second + "'";
                    if(sMax.first) { // has a max value
                        if(sMin == sMax.second) {
                            if(inclusiveMin && inclusiveMax) return "(" + lambdaVar + " == " + left + ")";
                            else return "False";
                        }
                        else {
                            return "(" + left + opMin + lambdaVar + opMax +
                                   right + ")";
                        }
                    } else {
                        return "(" + left + opMin + lambdaVar + ")";
                    }
                }
            }
        };

        struct IntervalCollection {
            std::vector<Interval> intervals;

            explicit IntervalCollection(std::vector<Interval> _intervals) : intervals(std::move(_intervals)) {}
            IntervalCollection() = default;

            /*!
             * Returns a string representing the filter condition implied by this interval collection
             * @param lambdaVar The variable whose intervals are represented by this IntervalCollection
             * @return string containing the python lambda for this interval collection
             */
            std::string createLambdaString(const std::string &lambdaVar) const {
                std::string condition;
                bool first = true;
                for(const auto &interval : intervals) {
                    if (first) first = false;
                    else condition += " or ";
                    condition += interval.createBooleanCondition(lambdaVar);
                }
                return condition;
            }

            // Intersect with a single Interval
            IntervalCollection intersectWith(const Interval &I) const {
                if (I.empty)
                    return {};

                std::vector<Interval> ret_intervals;
                for(const auto &J: intervals) {
                    auto t = J.intersectWith(I);
                    if(!t.empty) {
                        ret_intervals.push_back(t);
                    }
                }

                return IntervalCollection(ret_intervals);
            }

            IntervalCollection &logicalAnd(const IntervalCollection &other) {

                // special case: current collection empty => take other collection!
                if (intervals.empty()) {
                    intervals = other.intervals;
                    return *this;
                }

                // for each interval in this collection, get intersection with all intervals from other.
                // add result if non-empty interval!
                std::vector<Interval> res;
                for (const auto &I : other.intervals) {
                    auto m = intersectWith(I);
                    res.insert(res.end(), m.intervals.begin(), m.intervals.end());
                }
                intervals = res;

                return compact();
            }

            // @TODO: don't like the type design here...
            // ==> needs to get fixed!!!
            IntervalCollection &compact() {
                if (intervals.empty())
                    return *this;

                // https://www.geeksforgeeks.org/merging-intervals/
                std::sort(intervals.begin(), intervals.end(), [](const Interval &a, const Interval &b) {
                    assert(a.type == b.type);
                    if (a.type == python::Type::I64)
                        return a.iMin < b.iMin;
                    else if(a.type == python::Type::F64)
                        return a.dMin < b.dMin;
                    else return a.sMin < b.sMin;
                });

                int index = 0;
                for (int i = 1; i < intervals.size(); ++i) {
                    assert(intervals[i].type == python::Type::I64 || intervals[i].type == python::Type::STRING);

                    if(intervals[i].type == python::Type::I64) {
                        if (intervals[index].iMax >= intervals[i].iMin) {
                            intervals[index].iMax = std::max(intervals[index].iMax, intervals[i].iMax);
                            intervals[index].iMin = std::min(intervals[index].iMin, intervals[i].iMin);
                        } else {
                            index++;
                            intervals[index] = intervals[i];
                        }
                    } else if(intervals[i].type == python::Type::F64) {
                        if (intervals[index].dMax >= intervals[i].dMin) {
                            intervals[index].dMax = std::max(intervals[index].dMax, intervals[i].dMax);
                            intervals[index].dMin = std::min(intervals[index].dMin, intervals[i].dMin);
                        } else {
                            index++;
                            intervals[index] = intervals[i];
                        }
                    } else {
                        assert(intervals[i].type == python::Type::STRING);
                        if(!intervals[index].sMax.first || (intervals[index].sMax.second >= intervals[i].sMin)) {
                            // index has a larger upper bound than i's lower bound -> need to merge it in
                            intervals[index].sMax = std::max(intervals[index].sMax, intervals[i].sMax, Interval::stringUpperBoundLessThan);
                            intervals[index].sMin = std::min(intervals[index].sMin, intervals[i].sMin);
                        } else {
                            index++;
                            intervals[index] = intervals[i];
                        }
                    }
                }
                // 0...index-1 are the merged intervals
                int n = intervals.size();
                auto saved_index = index + 1;
                while (index + 1 < n) {
                    intervals.pop_back();
                    index++;
                }
                assert(intervals.size() == saved_index);
                return *this;
            }


            // or simple, because collection of intervals
            IntervalCollection &logicalOr(const IntervalCollection &other) {
                // simple, merge intervals
                for (auto I : other.intervals) {
                    assert(I.type == python::Type::I64 || I.type == python::Type::STRING);
                    intervals.push_back(I);
                }

                // compact intervals...
                return compact();
            }

            // invert all intervals
            IntervalCollection &logicalNot() {
                // compact first, so intervals are sorted!
                compact();

                // trick is to basically emit for each interval the complement. then the complements are intersected
                using namespace std;
                vector<Interval> complements;
                for (auto I : intervals) {
                    // type?
                    if (I.type == python::Type::I64) {
                        auto full_range = Interval::fullIntegerRange();
                        if(I.iMax == full_range.iMax && I.iMin == full_range.iMin) {
                            // full range? -> seems like an odd case; don't prefilter for now (e.g. this is basically not a condition)
                        } else {
                            if (I.iMax != full_range.iMax) { // add right complement
                                complements.emplace_back(I.iMax + 1, full_range.iMax);
                            }
                            if (I.iMin != full_range.iMin) { // add left complement
                                complements.emplace_back(full_range.iMin, I.iMin - 1);
                            }
                        }
                    } else if(I.type == python::Type::F64) {
                        auto full_range = Interval::fullDoubleRange();
                        if(I.dMax == full_range.dMax && I.dMin == full_range.dMin) {
                            // full range? -> seems like an odd case; don't prefilter for now (e.g. this is basically not a condition)
                        } else {
                            if (I.dMax != full_range.dMax) { // add right complement
                                complements.emplace_back(I.dMax + 1, full_range.dMax);
                            }
                            if (I.dMin != full_range.dMin) { // add left complement
                                complements.emplace_back(full_range.dMin, I.dMin - 1);
                            }
                        }
                    } else {
                        assert(I.type == python::Type::STRING);
                        complements.emplace_back("", I.sMin, true, !I.inclusiveMin);
                        if(I.sMax.first) { // bounded max
                            complements.emplace_back(I.sMax.second, !I.inclusiveMax);
                        }
                    }
                }

                intervals = complements;
                return *this;
            }

        };

        // set to track which variables to ignore in the postOrder (e.g. they occur within a compare or parameter list,
        // so don't generate a truth test for them)
        std::set<ASTNode*> _variablesToIgnore;

        // this is the basic element emitted for each ASTnode, can be empty too if no information is available yet...
        // lookup map of column -> index
        std::unordered_map<int64_t, IntervalCollection> _ranges;

        // stack to track current realization for an AST node
        std::stack<std::unordered_map<int64_t, IntervalCollection>> _rangesStack;
        std::unordered_map<std::string, int64_t> _columnToIndexMap;
        std::unordered_map<std::string, int64_t> _varNames;

        /*!
         * creates interval collection from comparing x op right
         */
        IntervalCollection fromCompare(const TokenType &op, ASTNode *right);

        static TokenType flipCompareOrder(const TokenType &op) {
            switch (op) {
                case TokenType::GREATER:
                    return TokenType::LESS;
                case TokenType::GREATEREQUAL:
                    return TokenType::LESSEQUAL;
                case TokenType::LESS:
                    return TokenType::GREATER;
                case TokenType::LESSEQUAL:
                    return TokenType::GREATEREQUAL;
                default:
                    return op;
            }
        }

        std::unordered_map<int64_t, IntervalCollection>
        static stripEmpty(const std::unordered_map<int64_t, IntervalCollection> &c) {
            std::unordered_map<int64_t, IntervalCollection> m;
            for (const auto &kv : c) {
                if (!kv.second.intervals.empty())
                    m[kv.first] = kv.second;
            }
            return m;
        }

        /*!
         * If [node] is an NFunction or NLambda, gets the variables' names (and sets [_varNames]).
         * @param node the node to check for variable names
         */
        void getVarNames(ASTNode *node);
        void ignoreVariable(ASTNode *node);

        /*!
         * Add [ic] to [variableRanges] if the passed in [subscript] is in the FilterBreakdownVisitor's recognized columns.
         * @param subscript the subscription to consider
         * @param ic the interval collection to be added
         * @param variableRanges the ranges the interval collection should be added to
         */
        void addICToSubscriptionIfValid(const NSubscription* subscript, const IntervalCollection &ic, std::unordered_map<int64_t, IntervalCollection> &variableRanges);

        /*!
         * Add [ic] to [variableRanges] if the passed in [identifier] is in the FilterBreakdownVisitor's recognized columns.
         * @param identifier the identifier to consider
         * @param ic the interval collection to be added
         * @param variableRanges the ranges the interval collection should be added to
         */
        void addICToIdentifierIfValid(const NIdentifier* identifier, const IntervalCollection &ic, std::unordered_map<int64_t, IntervalCollection> &variableRanges);

    protected:
        void postOrder(ASTNode *node) override;
        void preOrder(ASTNode *node) override;

    public:
        FilterBreakdownVisitor() {}
        std::unordered_map<int64_t, IntervalCollection> getRanges() const { return _rangesStack.top(); }
    };
}

#endif //TUPLEX_FILTERBREAKDOWNVISITOR_H