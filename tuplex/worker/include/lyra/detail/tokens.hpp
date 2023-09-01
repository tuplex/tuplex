// Copyright 2018-2019 René Ferdinand Rivera Morell
// Copyright 2017 Two Blue Cubes Ltd. All rights reserved.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef LYRA_DETAIL_TOKENS_HPP
#define LYRA_DETAIL_TOKENS_HPP

#include <string>
#include <vector>

namespace lyra { namespace detail {
// Wraps a token coming from a token stream. These may not directly
// correspond to strings as a single string may encode an option + its
// argument if the : or = form is used
enum class token_type
{
	unknown,
	option,
	argument
};

template <typename Char, class Traits = std::char_traits<Char>>
class basic_token_name
{
	public:
	using traits_type = Traits;
	using value_type = Char;
	using pointer = value_type *;
	using const_pointer = const value_type *;
	using reference = value_type &;
	using const_reference = const value_type &;
	using size_type = std::size_t;
	using difference_type = std::ptrdiff_t;
	using const_iterator = const_pointer;
	using iterator = const_iterator;
	using const_reverse_iterator = std::reverse_iterator<const_iterator>;
	using reverse_iterator = const_reverse_iterator;
	using string_type = std::basic_string<value_type, traits_type>;

	basic_token_name() noexcept
		: str { nullptr }
		, len { 0 }
	{}

	basic_token_name(const basic_token_name &) noexcept = default;

	basic_token_name(const_pointer s) noexcept
		: str { s }
		, len { traits_type::length(s) }
	{}

	basic_token_name(const_pointer s, size_type count) noexcept
		: str { s }
		, len { count }
	{}

	basic_token_name & operator=(const basic_token_name &) noexcept = default;

	void swap(basic_token_name & other) noexcept
	{
		auto tmp = *this;
		*this = other;
		other = tmp;
	}

	const_iterator begin() const noexcept { return this->str; }
	const_iterator end() const noexcept { return this->str + this->len; }
	const_iterator cbegin() const noexcept { return this->str; }
	const_iterator cend() const noexcept { return this->str + this->len; }

	size_type size() const noexcept { return this->len; }
	size_type length() const noexcept { return this->len; }
	bool empty() const noexcept { return this->len == 0; }

	friend string_type to_string(const basic_token_name & t)
	{
		return { t.str, t.len };
	}

	friend string_type operator+(
		const_pointer lhs, const basic_token_name & rhs)
	{
		return lhs + to_string(rhs);
	}

	private:
	const_pointer str;
	size_type len;
};

// using token_name = basic_token_name<std::string::value_type>;
using token_name = std::string;

struct token
{
	token_type type;
	token_name name;

	token()
		: type(token_type::unknown)
	{}
	token(const token & other) = default;
	token(token_type t, const token_name & n)
		: type(t)
		, name(n)
	{}

	explicit operator bool() const { return type != token_type::unknown; }
};

// Abstracts iterators into args with option arguments uniformly handled
class token_iterator
{
	public:
	template <typename Span>
	explicit token_iterator(Span const & args, std::string const & dels,
		std::string const & opt_prefix)
		: delimiters(dels)
		, option_prefix(opt_prefix)
		, args_i(args.begin())
		, args_e(args.end())
		, args_i_sub(1)
	{}

	explicit operator bool() const noexcept { return args_i != args_e; }

	token_iterator & pop(const token & arg_or_opt)
	{
		if (arg_or_opt.type == token_type::option && has_short_option_prefix())
		{
			// Multiple short options argument (-abc). Advance to the next
			// short option possible, or the next arg entirely.
			if (++args_i_sub >= args_i->size())
			{
				++args_i;
				args_i_sub = 1;
			}
		}
		else
		{
			// Regular arg or long option, just advance to the next arg.
			++args_i;
			args_i_sub = 1;
		}
		return *this;
	}

	token_iterator & pop(const token & /* opt */, const token & /* val */)
	{
		if (has_short_option_prefix() && args_i->size() > 2) ++args_i;
		else if (!has_value_delimiter())
			args_i += 2;
		else
			++args_i;
		args_i_sub = 1;
		return *this;
	}

	// Current arg looks like an option, short or long.
	bool has_option_prefix() const noexcept
	{
		return (args_i != args_e) && is_opt_prefix((*args_i)[0]);
	}

	// Current arg looks like a short option (-o).
	bool has_short_option_prefix() const noexcept
	{
		return (args_i != args_e) && is_opt_prefix((*args_i)[0])
			&& !is_opt_prefix((*args_i)[1]);
	}

	// Current arg looks like a long option (--option).
	bool has_long_option_prefix() const noexcept
	{
		return (args_i != args_e) && is_opt_prefix((*args_i)[0])
			&& is_opt_prefix((*args_i)[1]);
	}

	// Current arg looks like a delimited option+value (--option=x, -o=x)
	bool has_value_delimiter() const noexcept
	{
		return (args_i != args_e)
			&& (args_i->find_first_of(delimiters) != std::string::npos);
	}

	// Extract the current option token.
	token option() const
	{
		if (has_long_option_prefix())
		{
			if (has_value_delimiter())
				// --option=x
				return token(token_type::option,
					args_i->substr(0, args_i->find_first_of(delimiters)));
			else
				// --option
				return token(token_type::option, *args_i);
		}
		else if (has_short_option_prefix())
		{
			// -o (or possibly -abco)
			token t { token_type::option, option_prefix.substr(0, 1) };
			t.name += (*args_i)[args_i_sub];
			return t;
		}
		return token();
	}

	// Extracts the option value if available. This will do any needed
	// lookahead through the args for the value.
	token value() const
	{
		if (has_option_prefix() && has_value_delimiter())
			// --option=x, -o=x
			return token(token_type::argument,
				args_i->substr(args_i->find_first_of(delimiters) + 1));
		else if (has_long_option_prefix())
		{
			if (args_i + 1 != args_e)
				// --option x
				return token(token_type::argument, *(args_i + 1));
		}
		else if (has_short_option_prefix())
		{
			if (args_i_sub + 1 < args_i->size())
				// -ox
				return token(
					token_type::argument, args_i->substr(args_i_sub + 1));
			else if (args_i + 1 != args_e)
				// -o x
				return token(token_type::argument, *(args_i + 1));
		}
		return token();
	}

	token argument() const { return token(token_type::argument, *args_i); }

	private:
	std::string delimiters;
	std::string option_prefix;

	std::vector<std::string>::const_iterator args_i;
	std::vector<std::string>::const_iterator args_e;
	std::string::size_type args_i_sub;

	inline bool is_opt_prefix(char c) const noexcept
	{
		auto r = option_prefix.find(c) != std::string::npos;
		return r;
	}
};
}} // namespace lyra::detail

#endif
