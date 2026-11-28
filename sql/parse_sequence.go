package sql

import (
	"strconv"
	"strings"
)

func (p *Parser) parseCreateSequence() error {
	p.createSeq = CreateSequenceStmt{}
	p.advance() // SEQUENCE
	exists, err := p.parseIfNotExists()
	if err != nil {
		return err
	}
	p.createSeq.IfNotExists = exists
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.createSeq.Name, p.createSeq.Pos = name, pos
	opts, err := p.parseSequenceOptions(false, false)
	if err != nil {
		return err
	}
	p.createSeq.Options = opts
	return p.expectEnd()
}

func (p *Parser) parseAlterSequence() error {
	p.alterSeq = AlterSequenceStmt{}
	p.advance() // SEQUENCE
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
		p.alterSeq.IfExists = true
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.alterSeq.Name, p.alterSeq.Pos = name, pos
	opts, err := p.parseSequenceOptions(true, false)
	if err != nil {
		return err
	}
	p.alterSeq.Options = opts
	return p.expectEnd()
}

func (p *Parser) parseDropSequence() error {
	p.dropSeq = DropSequenceStmt{}
	p.advance() // SEQUENCE
	if p.acceptKeyword(kwIf) {
		if err := p.expectKeyword(kwExists, "EXISTS after IF"); err != nil {
			return err
		}
		p.dropSeq.IfExists = true
	}
	name, pos, err := p.parseCollectionName()
	if err != nil {
		return err
	}
	p.dropSeq.Name, p.dropSeq.Pos = name, pos
	multiple := false
	for p.tok.kind == tokComma {
		multiple = true
		p.advance()
		if _, _, err := p.parseCollectionName(); err != nil {
			return err
		}
	}
	behavior := p.acceptDropBehavior()
	if behavior == dropRestrict {
		p.dropSeq.Restrict = true
	}
	if multiple || behavior == dropCascade {
		if err := p.expectEnd(); err != nil {
			return err
		}
		if multiple {
			return p.featureNotSupportedHere("DROP SEQUENCE of multiple sequences is not supported")
		}
		return p.featureNotSupportedHere("DROP SEQUENCE CASCADE is not supported; remove dependencies explicitly")
	}
	return p.expectEnd()
}

func (p *Parser) parseSequenceOptions(allowRestart bool, stopAtRParen bool) (SequenceOptions, error) {
	var opts SequenceOptions
	for {
		if p.tok.kind == tokEOF || p.tok.kind == tokSemicolon || (stopAtRParen && p.tok.kind == tokRParen) {
			break
		}
		switch {
		case p.atKeyword(kwAs):
			if opts.HasDataType {
				return opts, p.errHere("conflicting or redundant AS option in sequence")
			}
			p.advance()
			dt, err := p.parseSequenceDataType()
			if err != nil {
				return opts, err
			}
			opts.HasDataType = true
			opts.DataType = dt

		case tokenTextEqual(p.tok, "INCREMENT"):
			if opts.HasIncrement {
				return opts, p.errHere("conflicting or redundant INCREMENT option in sequence")
			}
			p.advance()
			if p.atKeyword(kwBy) {
				p.advance()
			}
			val, err := p.parseSignedInt64("INCREMENT value")
			if err != nil {
				return opts, err
			}
			opts.HasIncrement = true
			opts.Increment = val

		case tokenTextEqual(p.tok, "MINVALUE"):
			if opts.HasMinValue || opts.NoMinValue {
				return opts, p.errHere("conflicting or redundant MINVALUE option in sequence")
			}
			p.advance()
			val, err := p.parseSignedInt64("MINVALUE")
			if err != nil {
				return opts, err
			}
			opts.HasMinValue = true
			opts.MinValue = val

		case tokenTextEqual(p.tok, "MAXVALUE"):
			if opts.HasMaxValue || opts.NoMaxValue {
				return opts, p.errHere("conflicting or redundant MAXVALUE option in sequence")
			}
			p.advance()
			val, err := p.parseSignedInt64("MAXVALUE")
			if err != nil {
				return opts, err
			}
			opts.HasMaxValue = true
			opts.MaxValue = val

		case tokenTextEqual(p.tok, "START"):
			if opts.HasStart {
				return opts, p.errHere("conflicting or redundant START option in sequence")
			}
			p.advance()
			if p.atKeyword(kwWith) {
				p.advance()
			}
			val, err := p.parseSignedInt64("START value")
			if err != nil {
				return opts, err
			}
			opts.HasStart = true
			opts.Start = val

		case tokenTextEqual(p.tok, "RESTART"):
			if !allowRestart {
				return opts, p.errHere("RESTART is only valid in ALTER SEQUENCE")
			}
			if opts.HasRestart {
				return opts, p.errHere("conflicting or redundant RESTART option in sequence")
			}
			p.advance()
			opts.HasRestart = true
			if p.atKeyword(kwWith) {
				p.advance()
				val, err := p.parseSignedInt64("RESTART value")
				if err != nil {
					return opts, err
				}
				opts.HasRestartValue = true
				opts.RestartValue = val
			} else if p.tok.kind == tokNumber || p.tok.kind == tokMinus || p.tok.kind == tokPlus {
				val, err := p.parseSignedInt64("RESTART value")
				if err != nil {
					return opts, err
				}
				opts.HasRestartValue = true
				opts.RestartValue = val
			}

		case tokenTextEqual(p.tok, "CACHE"):
			if opts.HasCache {
				return opts, p.errHere("conflicting or redundant CACHE option in sequence")
			}
			p.advance()
			val, err := p.parseSignedInt64("CACHE value")
			if err != nil {
				return opts, err
			}
			opts.HasCache = true
			opts.Cache = val

		case tokenTextEqual(p.tok, "CYCLE"):
			if opts.HasCycle {
				return opts, p.errHere("conflicting or redundant CYCLE option in sequence")
			}
			p.advance()
			opts.HasCycle = true
			opts.Cycle = true

		case tokenTextEqual(p.tok, "NO"):
			p.advance()
			switch {
			case tokenTextEqual(p.tok, "MINVALUE"):
				if opts.HasMinValue || opts.NoMinValue {
					return opts, p.errHere("conflicting or redundant MINVALUE option in sequence")
				}
				p.advance()
				opts.NoMinValue = true
			case tokenTextEqual(p.tok, "MAXVALUE"):
				if opts.HasMaxValue || opts.NoMaxValue {
					return opts, p.errHere("conflicting or redundant MAXVALUE option in sequence")
				}
				p.advance()
				opts.NoMaxValue = true
			case tokenTextEqual(p.tok, "CYCLE"):
				if opts.HasCycle {
					return opts, p.errHere("conflicting or redundant CYCLE option in sequence")
				}
				p.advance()
				opts.HasCycle = true
				opts.Cycle = false
			default:
				return opts, p.errHere("expected MINVALUE, MAXVALUE, or CYCLE after NO")
			}

		default:
			return opts, p.errHere("unexpected token in sequence options")
		}
	}
	return opts, nil
}

func (p *Parser) parseSequenceDataType() (string, error) {
	if p.tok.kind != tokIdent {
		return "", p.errHere("expected SMALLINT, INTEGER, or BIGINT after AS")
	}
	upper := strings.ToUpper(p.tok.text)
	p.advance()
	switch upper {
	case "SMALLINT", "INT2":
		return "SMALLINT", nil
	case "INTEGER", "INT", "INT4":
		return "INTEGER", nil
	case "BIGINT", "INT8":
		return "BIGINT", nil
	default:
		return "", p.errfHere("sequence type must be SMALLINT, INTEGER, or BIGINT, got %s", upper)
	}
}

func (p *Parser) parseSignedInt64(context string) (int64, error) {
	neg := false
	if p.tok.kind == tokMinus {
		neg = true
		p.advance()
	} else if p.tok.kind == tokPlus {
		p.advance()
	}
	if p.tok.kind != tokNumber {
		return 0, p.errfHere("expected integer for %s", context)
	}
	text := p.tok.text
	pos := p.tok.pos
	p.advance()
	if neg {
		text = "-" + text
	}
	val, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, p.errfAt(pos, "invalid 64-bit integer %q for %s", text, context)
	}
	return val, nil
}
