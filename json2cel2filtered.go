package json2cel2filtered

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"

	"github.com/google/cel-go/cel"
	tr "github.com/google/cel-go/common/types/ref"
)

var (
	ErrNotBool error = errors.New("not bool type")
)

type EnvOpts []cel.EnvOption

var EnvOptsEmpty EnvOpts

func (o EnvOpts) ToEnv() (CelEnv, error) {
	env, e := cel.NewEnv(o...)
	return CelEnv{Env: env}, e
}

type AstValidators []cel.ASTValidator

func (v AstValidators) ToEnvOption() cel.EnvOption {
	return cel.ASTValidators(v...)
}

type CelEnv struct{ *cel.Env }

func (r CelEnv) Compile(txt string) (CelAst, error) {
	ast, issues := r.Env.Compile(txt)
	if nil != issues && nil != issues.Err() {
		return CelAst{}, issues.Err()
	}

	return CelAst{Ast: ast}, nil
}

func (r CelEnv) CreateProgram(ast CelAst, opts ...cel.ProgramOption) (CelProgram, error) {
	prog, e := r.Env.Program(ast.Ast, opts...)
	return CelProgram{Program: prog}, e
}

type CelAst struct{ *cel.Ast }

func (a CelAst) ToChecked(env CelEnv) (CelAst, error) {
	ast, issues := env.Env.Check(a.Ast)
	if nil != issues && nil != issues.Err() {
		return CelAst{}, issues.Err()
	}

	return CelAst{Ast: ast}, nil
}

type EvalOpts []cel.EvalOption

func (o EvalOpts) ToProgramOption() cel.ProgramOption {
	return cel.EvalOptions(o...)
}

type CostLimit uint64

func (l CostLimit) ToProgramOption() cel.ProgramOption {
	return cel.CostLimit(uint64(l))
}

type CheckFrequency uint

func (f CheckFrequency) ToProgramOption() cel.ProgramOption {
	return cel.InterruptCheckFrequency(uint(f))
}

type CelProgram struct{ cel.Program }

func (p CelProgram) Eval(ctx context.Context, input any) (tr.Val, *cel.EvalDetails, error) {
	return p.Program.ContextEval(ctx, input)
}

func (p CelProgram) Filter(ctx context.Context, input any) (keep bool, e error) {
	v, _, e := p.Eval(ctx, input)
	if nil != e {
		return false, e
	}

	keep, e = CelValue{Val: v}.ToBool()
	return
}

type CelValue struct{ tr.Val }

func (v CelValue) ToBool() (bool, error) {
	var a any = v.Val.Value()
	switch t := a.(type) {
	case bool:
		return t, nil
	default:
		return false, ErrNotBool
	}
}

type JsonLine = []byte

type JsonLines iter.Seq[JsonLine]

func (l JsonLines) ToMap() iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}
		for line := range l {
			clear(buf)
			e := json.Unmarshal(line, &buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

type JsonReader struct{ io.Reader }

func (r JsonReader) ToMaps() iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var br io.Reader = bufio.NewReader(r.Reader)
		var dec *json.Decoder = json.NewDecoder(br)

		buf := map[string]any{}
		for {
			clear(buf)

			e := dec.Decode(&buf)
			if io.EOF == e {
				return
			}

			if !yield(buf, e) {
				return
			}
		}
	}
}

type JsonMaps iter.Seq2[map[string]any, error]

func (m JsonMaps) ToFiltered(
	ctx context.Context,
	prog CelProgram,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		item := map[string]any{}
		for obj, err := range m {
			clear(item)
			if nil != err {
				yield(nil, err)
				return
			}

			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			item["item"] = obj
			keep, err := prog.Filter(ctx, item)
			if nil != err {
				yield(nil, err)
				return
			}

			if !keep {
				continue
			}

			if !yield(obj, nil) {
				return
			}
		}
	}
}

type JsonWriter struct{ io.Writer }

func (w JsonWriter) WriteAll(ctx context.Context, jmaps JsonMaps) error {
	var bwtr *bufio.Writer = bufio.NewWriter(w.Writer)
	var enc *json.Encoder = json.NewEncoder(bwtr)
	for jobj, e := range jmaps {
		if nil != e {
			return e
		}

		e := enc.Encode(jobj)
		if nil != e {
			return e
		}
	}
	return bwtr.Flush()
}
