//
// Created by Leonhard Spiegelberg on 9/15/22.
//

#include <JSONUtils.h>

namespace tuplex {
    // use simdjson for escaping

    std::string unescape_json_string(const std::string& json_string) {
        if(json_string.empty())
            return "";

        if(json_string.size() < 2)
            throw std::runtime_error("JSON string must have at least two characters \"\"");
        if(json_string.front() != '"' || json_string.back() != '"')
            throw std::runtime_error("not a valid JSON string");

        // unescape...

        // basically revert the routine from below...
        std::stringstream out;
        for(unsigned i = 1; i < json_string.length() - 1; ++i) {
            switch(json_string[i]) {
                case '\\': {
                    if(i + 1 < json_string.length() - 1)
                        ++i;
                    switch(json_string[i]) {
                        case 'b': {
                            out<<'\b';
                            break;
                        }
                        case 'f': {
                            out<<'\f';
                            break;
                        }
                        case 'n': {
                            out<<'\n';
                            break;
                        }
                        case 'r': {
                            out<<'\r';
                            break;
                        }
                        case '"': {
                            out<<'"';
                            break;
                        }
                        case 't': {
                            out<<'\t';
                            break;
                        }
                        case '\\': {
                            out<<'\\';
                            break;
                        }
                        case 'u': {
                            // unicode decode...
                            // 4 more chars to come -> int value
                            if(i + 4 < json_string.length() - 1) {
                                // char buf[5] = {json_string[i + 1], json_string[i + 2], json_string[i + 3], json_string[i + 4], '\0'};
                                char buf[5] = {'0', 'x', json_string[i + 3], json_string[i + 4], '\0'};
                                char c = strtol(buf, nullptr, 16) & 0xff;
                                out<<c;
                                i += 4;
                            } else {
                                // internal error
                                return out.str();
                            }
                            break;
                        }
                        default:
                            // internal error...
                            return out.str();
                    }
                    break;
                }
                default: {
                    out<<json_string[i];
                    break;
                }
            }
        }
        return out.str();
    }

    std::string escape_json_string(const std::string& string) {
        // can use the following routine from SIMDJSON:
        // source: https://github.com/simdjson/simdjson/blob/d036fdf9197f4dce60465e32ea4a82e75c271871/include/simdjson/internal/jsonformatutils.h
        // inline std::ostream& operator<<(std::ostream& out, const escape_json_string &unescaped) {
        //  for (size_t i=0; i<unescaped.str.length(); i++) {
        //    switch (unescaped.str[i]) {
        //    case '\b':
        //      out << "\\b";
        //      break;
        //    case '\f':
        //      out << "\\f";
        //      break;
        //    case '\n':
        //      out << "\\n";
        //      break;
        //    case '\r':
        //      out << "\\r";
        //      break;
        //    case '\"':
        //      out << "\\\"";
        //      break;
        //    case '\t':
        //      out << "\\t";
        //      break;
        //    case '\\':
        //      out << "\\\\";
        //      break;
        //    default:
        //      if (static_cast<unsigned char>(unescaped.str[i]) <= 0x1F) {
        //        // TODO can this be done once at the beginning, or will it mess up << char?
        //        std::ios::fmtflags f(out.flags());
        //        out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << int(unescaped.str[i]);
        //        out.flags(f);
        //      } else {
        //        out << unescaped.str[i];
        //      }
        //    }
        //  }
        //  return out;
        //}

        std::stringstream out;
        out<<'"';
        for (size_t i=0; i< string.length(); i++) {
            switch (string[i]) {
                case '\b':
                    out << "\\b";
                    break;
                case '\f':
                    out << "\\f";
                    break;
                case '\n':
                    out << "\\n";
                    break;
                case '\r':
                    out << "\\r";
                    break;
                case '\"':
                    out << "\\\"";
                    break;
                case '\t':
                    out << "\\t";
                    break;
                case '\\':
                    out << "\\\\";
                    break;
                default:
                    if (static_cast<unsigned char>(string[i]) <= 0x1F) {
                        // TODO can this be done once at the beginning, or will it mess up << char?
                        std::ios::fmtflags f(out.flags());
                        out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << int(string[i]);
                        out.flags(f);
                    } else {
                        out << string[i];
                    }
            }
        }

        out<<'"';
        return out.str();
    }
}