package main

import (
	"context"
	"errors"
	"flag"
	"iter"
	"log"
	"os"

	"github.com/google/cel-go/cel"
	ct "github.com/google/cel-go/common/types"
	jc "github.com/takanoriyanagitani/go-json-filter-cel"
)

var eopts jc.EnvOpts = []cel.EnvOption{
	cel.Variable("item", ct.MapType),
}

var evalOpts jc.EvalOpts = []cel.EvalOption{
	cel.OptOptimize,
	cel.OptTrackCost,
}

const (
	defaultCostLimit uint64 = 65535
	defaultCheckFreq uint   = 1023
)

var ErrNoExpr error = errors.New("missing expression")

func parseFlagsAndSetup() (string, jc.CostLimit, jc.CheckFrequency, error) {
	var celExprVal string
	flag.StringVar(&celExprVal, "expr", "", "CEL expression to filter JSON(required)")

	var costLimitVal jc.CostLimit
	var checkFreqVal jc.CheckFrequency

	flag.Uint64Var(
		(*uint64)(&costLimitVal),
		"cost-limit",
		defaultCostLimit,
		"max cost for a single evaluation",
	)
	flag.UintVar(
		(*uint)(&checkFreqVal),
		"check-freq",
		defaultCheckFreq,
		"cost check frequency",
	)

	flag.Parse()

	if len(celExprVal) < 1 {
		flag.Usage()
		return "", 0, 0, ErrNoExpr
	}

	return celExprVal, costLimitVal, checkFreqVal, nil
}

func createCelProgram(
	celExprVal string,
	costLimitVal jc.CostLimit,
	checkFreqVal jc.CheckFrequency,
	eopts jc.EnvOpts,
) (jc.CelProgram, error) {
	var progOpts []cel.ProgramOption = []cel.ProgramOption{
		evalOpts.ToProgramOption(),
		costLimitVal.ToProgramOption(),
		checkFreqVal.ToProgramOption(),
		cel.EvalOptions(cel.OptPartialEval),
	}

	env, err := eopts.ToEnv()
	if nil != err {
		return jc.CelProgram{}, err
	}

	ast, err := env.Compile(celExprVal)
	if nil != err {
		return jc.CelProgram{}, err
	}

	checked, err := ast.ToChecked(env)
	if nil != err {
		return jc.CelProgram{}, err
	}

	prog, err := env.CreateProgram(checked, progOpts...)
	if nil != err {
		return jc.CelProgram{}, err
	}

	return prog, nil
}

func sub() error {
	celExprVal, costLimitVal, checkFreqVal, err := parseFlagsAndSetup()
	if nil != err {
		return err
	}

	prog, err := createCelProgram(celExprVal, costLimitVal, checkFreqVal, eopts)
	if nil != err {
		return err
	}

	var jsonReader jc.JsonReader = jc.JsonReader{Reader: os.Stdin}
	var jsonMaps jc.JsonMaps = jc.JsonMaps(jsonReader.ToMaps())

	var filtered iter.Seq2[map[string]any, error] = jsonMaps.
		ToFiltered(
			context.Background(),
			prog,
		)

	var jwtr jc.JsonWriter = jc.JsonWriter{Writer: os.Stdout}
	return jwtr.WriteAll(
		context.Background(),
		jc.JsonMaps(filtered),
	)
}

func main() {
	e := sub()
	if nil != e {
		log.Fatalf("%v\n", e)
	}
}
