package sql

// builtinScalarFuncName recognizes standard PostgreSQL scalar function names
// with zero allocations. A match is only parsed as a function call when
// immediately followed by '(', so bare field names like "length", "left",
// "right", "trim", or "lower" remain ordinary JSON paths.
func builtinScalarFuncName(tok token) (string, bool) {
	if tok.kind != tokIdent || len(tok.text) < 2 || len(tok.text) > 25 {
		return "", false
	}
	s := tok.text
	switch s[0] | 0x20 {
	case 'a':
		switch {
		case equalFoldASCII(s, "abs"):
			return "abs", true
		case equalFoldASCII(s, "array_length"):
			return "array_length", true
		case equalFoldASCII(s, "array_to_string"):
			return "array_to_string", true
		case equalFoldASCII(s, "ascii"):
			return "ascii", true
		}
	case 'b':
		if equalFoldASCII(s, "btrim") {
			return "btrim", true
		}
	case 'c':
		switch {
		case equalFoldASCII(s, "cardinality"):
			return "cardinality", true
		case equalFoldASCII(s, "ceil"):
			return "ceil", true
		case equalFoldASCII(s, "ceiling"):
			return "ceiling", true
		case equalFoldASCII(s, "char_length"):
			return "char_length", true
		case equalFoldASCII(s, "character_length"):
			return "character_length", true
		case equalFoldASCII(s, "chr"):
			return "chr", true
		case equalFoldASCII(s, "clock_timestamp"):
			return "clock_timestamp", true
		case equalFoldASCII(s, "concat"):
			return "concat", true
		case equalFoldASCII(s, "concat_ws"):
			return "concat_ws", true
		case equalFoldASCII(s, "current_date"):
			return "current_date", true
		case equalFoldASCII(s, "current_timestamp"):
			return "current_timestamp", true
		case equalFoldASCII(s, "currval"):
			return "currval", true
		}
	case 'd':
		if equalFoldASCII(s, "date_trunc") {
			return "date_trunc", true
		}
	case 'e':
		switch {
		case equalFoldASCII(s, "exp"):
			return "exp", true
		case equalFoldASCII(s, "extract"):
			return "extract", true
		}
	case 'f':
		if equalFoldASCII(s, "floor") {
			return "floor", true
		}
	case 'g':
		if equalFoldASCII(s, "gen_random_uuid") {
			return "gen_random_uuid", true
		}
	case 'i':
		if equalFoldASCII(s, "initcap") {
			return "initcap", true
		}
	case 'j':
		switch {
		case equalFoldASCII(s, "json_typeof"):
			return "json_typeof", true
		case equalFoldASCII(s, "jsonb_typeof"):
			return "jsonb_typeof", true
		case equalFoldASCII(s, "json_array_length"):
			return "json_array_length", true
		case equalFoldASCII(s, "jsonb_array_length"):
			return "jsonb_array_length", true
		case equalFoldASCII(s, "json_build_object"):
			return "json_build_object", true
		case equalFoldASCII(s, "jsonb_build_object"):
			return "jsonb_build_object", true
		case equalFoldASCII(s, "json_build_array"):
			return "json_build_array", true
		case equalFoldASCII(s, "jsonb_build_array"):
			return "jsonb_build_array", true
		case equalFoldASCII(s, "json_extract_path"):
			return "json_extract_path", true
		case equalFoldASCII(s, "jsonb_extract_path"):
			return "jsonb_extract_path", true
		case equalFoldASCII(s, "json_extract_path_text"):
			return "json_extract_path_text", true
		case equalFoldASCII(s, "jsonb_extract_path_text"):
			return "jsonb_extract_path_text", true
		case equalFoldASCII(s, "json_strip_nulls"):
			return "json_strip_nulls", true
		case equalFoldASCII(s, "jsonb_strip_nulls"):
			return "jsonb_strip_nulls", true
		case equalFoldASCII(s, "jsonb_pretty"):
			return "jsonb_pretty", true
		case equalFoldASCII(s, "jsonb_set"):
			return "jsonb_set", true
		case equalFoldASCII(s, "json_set"):
			return "json_set", true
		}
	case 'l':
		switch {
		case equalFoldASCII(s, "lastval"):
			return "lastval", true
		case equalFoldASCII(s, "left"):
			return "left", true
		case equalFoldASCII(s, "length"):
			return "length", true
		case equalFoldASCII(s, "ln"):
			return "ln", true
		case equalFoldASCII(s, "log"):
			return "log", true
		case equalFoldASCII(s, "log10"):
			return "log10", true
		case equalFoldASCII(s, "lower"):
			return "lower", true
		case equalFoldASCII(s, "lpad"):
			return "lpad", true
		case equalFoldASCII(s, "ltrim"):
			return "ltrim", true
		}
	case 'm':
		switch {
		case equalFoldASCII(s, "md5"):
			return "md5", true
		case equalFoldASCII(s, "mod"):
			return "mod", true
		}
	case 'n':
		switch {
		case equalFoldASCII(s, "nextval"):
			return "nextval", true
		case equalFoldASCII(s, "now"):
			return "now", true
		}
	case 'o':
		if equalFoldASCII(s, "octet_length") {
			return "octet_length", true
		}
	case 'p':
		switch {
		case equalFoldASCII(s, "pi"):
			return "pi", true
		case equalFoldASCII(s, "pow"):
			return "pow", true
		case equalFoldASCII(s, "power"):
			return "power", true
		}
	case 'r':
		switch {
		case equalFoldASCII(s, "regexp_count"):
			return "regexp_count", true
		case equalFoldASCII(s, "regexp_like"):
			return "regexp_like", true
		case equalFoldASCII(s, "regexp_replace"):
			return "regexp_replace", true
		case equalFoldASCII(s, "regexp_substr"):
			return "regexp_substr", true
		case equalFoldASCII(s, "repeat"):
			return "repeat", true
		case equalFoldASCII(s, "replace"):
			return "replace", true
		case equalFoldASCII(s, "reverse"):
			return "reverse", true
		case equalFoldASCII(s, "right"):
			return "right", true
		case equalFoldASCII(s, "round"):
			return "round", true
		case equalFoldASCII(s, "rpad"):
			return "rpad", true
		case equalFoldASCII(s, "rtrim"):
			return "rtrim", true
		}
	case 's':
		switch {
		case equalFoldASCII(s, "setval"):
			return "setval", true
		case equalFoldASCII(s, "sign"):
			return "sign", true
		case equalFoldASCII(s, "split_part"):
			return "split_part", true
		case equalFoldASCII(s, "sqrt"):
			return "sqrt", true
		case equalFoldASCII(s, "starts_with"):
			return "starts_with", true
		case equalFoldASCII(s, "statement_timestamp"):
			return "statement_timestamp", true
		case equalFoldASCII(s, "string_to_array"):
			return "string_to_array", true
		case equalFoldASCII(s, "strpos"):
			return "strpos", true
		case equalFoldASCII(s, "substr"):
			return "substr", true
		case equalFoldASCII(s, "substring"):
			return "substring", true
		}
	case 't':
		switch {
		case equalFoldASCII(s, "to_json"):
			return "to_json", true
		case equalFoldASCII(s, "to_jsonb"):
			return "to_jsonb", true
		case equalFoldASCII(s, "translate"):
			return "translate", true
		case equalFoldASCII(s, "trim"):
			return "trim", true
		case equalFoldASCII(s, "trunc"):
			return "trunc", true
		}
	case 'u':
		if equalFoldASCII(s, "upper") {
			return "upper", true
		}
	}
	return "", false
}

func (p *Parser) parseBuiltinScalarFunc(name string, ctx scalarExprContext) (*ScalarExpr, error) {
	head := p.tok
	p.advance()
	if p.tok.kind != tokLParen {
		path, err := p.continuePath(head, ctx == scalarSelect)
		if err != nil {
			return nil, err
		}
		node := p.newScalar(ScalarPath, head.pos)
		node.Path = path
		return node, nil
	}
	p.advance() // '('
	var args []ScalarWhen
	if p.tok.kind != tokRParen {
		for {
			if len(args) == 1024 {
				return nil, p.errHere("a scalar function accepts at most 1024 arguments")
			}
			// Support PostgreSQL EXTRACT(field FROM expr) syntax.
			if name == "extract" && len(args) == 0 && p.tok.kind == tokIdent {
				fieldTok := p.tok
				p.advance()
				if p.atKeyword(kwFrom) {
					p.advance()
					lit := p.newScalar(ScalarLiteral, fieldTok.pos)
					lit.Value = Operand{Kind: OperandString, Text: fieldTok.text, Pos: fieldTok.pos}
					args = append(args, ScalarWhen{Result: lit, Pos: fieldTok.pos})
					arg, err := p.parseScalarExpression(ctx)
					if err != nil {
						return nil, err
					}
					args = append(args, ScalarWhen{Result: arg, Pos: arg.Pos})
					break
				}
				path, err := p.continuePath(fieldTok, ctx == scalarSelect)
				if err != nil {
					return nil, err
				}
				arg := p.newScalar(ScalarPath, fieldTok.pos)
				arg.Path = path
				args = append(args, ScalarWhen{Result: arg, Pos: arg.Pos})
			} else {
				arg, err := p.parseScalarExpression(ctx)
				if err != nil {
					return nil, err
				}
				args = append(args, ScalarWhen{Result: arg, Pos: arg.Pos})
			}
			if p.tok.kind != tokComma {
				break
			}
			p.advance()
		}
	}
	if err := p.expect(tokRParen, "')' after function arguments"); err != nil {
		return nil, err
	}
	node := p.newScalar(ScalarFunc, head.pos)
	node.Value = Operand{Kind: OperandString, Text: name, Pos: head.pos}
	node.Whens = args
	return node, nil
}
