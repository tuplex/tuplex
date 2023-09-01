//
// Created by Leonhard Spiegelberg on 7/11/22.
//

#include <JSONUtils.h>

namespace tuplex {

    static size_t escape_space_required_for_json(const std::string &str) {
        size_t size = 0;
        for(const auto &c: str) {
            switch(c) {
                case '"':
                case '\\':
                case '\b':
                case '\f':
                case '\n':
                case '\r':
                case '\t': {
                    // From c (1 byte) to hex (2 bytes).
                    size += 1;
                    break;
                }

                default: {
                    if(c >= 0x00 && c <= 0x1f) {
                        // From c (1 byte) to unicode hex (6 bytes).
                        size += 5;
                    }
                    break;
                }
            }
        }
        return size;
    }

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

     // adapted from https://fossies.org/linux/envoy/source/common/common/json_escape_string.h
    std::string escape_for_json(const std::string& str) {

        return escape_json_string(str);

        // create a copy
        std::string res(str.size() + 1 + escape_space_required_for_json(str), '\\');

        size_t position = 0;
        // go through and escape
        for (const auto& c: str) {
            switch(c) {
                case '\0':
                    res[position] = '\0';
                    return res;
                case '"':
                    // Quotation mark (0x22).
                    assert(position + 1 < res.size());
                    res[position + 1] = '"';
                    position += 2;
                    break;
                case '\\':
                    // Reverse solidus (0x5c).
                    // Nothing to change.
                    position += 2;
                    break;
                case '\b':
                    // Backspace (0x08).
                    assert(position + 1 < res.size());
                    res[position + 1] = 'b';
                    position += 2;
                    break;
                case '\f':
                    // Form feed (0x0c).
                    assert(position + 1 < res.size());
                    res[position + 1] = 'f';
                    position += 2;
                    break;
                case '\n':
                    // Newline (0x0a).
                    assert(position + 1 < res.size());
                    res[position + 1] = 'n';
                    position += 2;
                    break;
                case '\r':
                    // Carriage return (0x0d).
                    assert(position + 1 < res.size());
                    res[position + 1] = 'r';
                    position += 2;
                    break;
                case '\t':
                    // Horizontal tab (0x09).
                    assert(position + 1 < res.size());
                    res[position + 1] = 't';
                    position += 2;
                    break;
                default:
                    if (c >= 0x00 && c <= 0x1f) {
                        // Print c as unicode hex.
                        assert(position + 1 < res.size());
                        sprintf(&res[position + 1], "u%04x", static_cast<uint32_t>(c));
                        position += 6;
                        // Overwrite trailing null c.
                        res[position] = '\\';
                    } else {
                        // All other cs are added as-is.
                        assert(position + 1 < res.size());
                        res[position++] = c;
                    }
                    break;
            }
        }
        res[position] = '\0';
        return res;
    }
}