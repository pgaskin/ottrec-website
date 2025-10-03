// Package pflagx implements extensions to pflag.
package pflagx

import (
	"log/slog"

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
