// Package pflagx implements extensions to pflag.
package pflagx

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"unicode"

	"github.com/spf13/pflag"
)

type FlagSet pflag.FlagSet

func FlagSetExt(fs *pflag.FlagSet) *FlagSet {
	return (*FlagSet)(fs)
}

func (fs *FlagSet) FlagSet() *pflag.FlagSet {
	return (*pflag.FlagSet)(fs)
}

func LevelP(name, shorthand string, value slog.Level, usage string) *slog.LevelVar {
	return FlagSetExt(pflag.CommandLine).LevelP(name, shorthand, value, usage)
}

func (fs *FlagSet) LevelP(name, shorthand string, value slog.Level, usage string) *slog.LevelVar {
	level := new(slog.LevelVar)
	def := new(slog.LevelVar)
	def.Set(value)
	pflag.TextVarP(level, name, shorthand, def, usage)
	return level
}

func ParseEnv(prefix string) {
	FlagSetExt(pflag.CommandLine).ParseEnv(prefix)
}

func (fs *FlagSet) ParseEnv(prefix string) {
	for _, env := range os.Environ() {
		if k, v, ok := strings.Cut(env, "="); ok {
			if s, ok := strings.CutPrefix(k, prefix); ok {
				n := strings.Map(func(r rune) rune {
					switch r {
					case '_':
						return '-'
					}
					return unicode.ToLower(r)
				}, s)
				f := pflag.CommandLine.Lookup(n)
				if f == nil {
					fmt.Fprintf(fs.FlagSet().Output(), "env %s: unknown flag --%s\n", k, n)
					continue
				}
				if err := f.Value.Set(v); err != nil {
					fmt.Fprintf(fs.FlagSet().Output(), "env %s: flag --%s: invalid argument: %v\n", k, n, err)
					os.Exit(2)
				}
			}
		}
	}
}
