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

    // adapted from https://fossies.org/linux/envoy/source/common/common/json_escape_string.h
    std::string escape_for_json(const std::string& str) {
        // create a copy
        std::string res(str.size() + 1 + escape_space_required_for_json(str), '\\');

        size_t position = 0;
        // go through and escape
        for (const auto& c: str) {
            switch(c) {
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

        return res;
    }
}