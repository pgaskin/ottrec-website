# Contributing

## Development

#### Workspace setup

```bash
# create a folder
mkdir ottrec
cd ottrec

# clone the repositories
git clone https://github.com/pgaskin/ottrec ottrec
git clone https://github.com/pgaskin/ottrec-website website
git clone https://github.com/pgaskin/ottrec-data data --filter=blob:none
git -C data worktree add ../cache cache

# set up the go workspace
go work init
go work use ./ottrec
go work use ./website

# optional: define some useful aliases
alias ottrec-root='dirname "$(go env GOWORK)"'
alias croot='cd "$(ottrec-root)"'
```

#### VSCode extensions

- [`golang.go`](https://marketplace.visualstudio.com/items?itemName=golang.Go)
- [`a-h.templ`](https://marketplace.visualstudio.com/items?itemName=a-h.templ)
- [`pbkit.vscode-pbkit`](https://marketplace.visualstudio.com/items?itemName=pbkit.vscode-pbkit)
- [`aaron-bond.better-comments`](https://marketplace.visualstudio.com/items?itemName=aaron-bond.better-comments)
- [`dnut.rewrap-revived`](https://marketplace.visualstudio.com/items?itemName=dnut.rewrap-revived) - for rewrapping comments (Alt+A)
- [`yo1dog.cursor-align`](https://marketplace.visualstudio.com/items?itemName=yo1dog.cursor-align)

#### VSCode settings

```jsonc
{
    "[go]": {
        "editor.codeActionsOnSave": {
            "source.organizeImports": "explicit"
        },
        "editor.formatOnSave": true,
    },
    "[templ]": {
        "editor.defaultFormatter": "a-h.templ",
        "editor.formatOnSave": true,
        "editor.wordWrap": "on",
    },
}
```

### Scraper

#### Running the unit tests

```bash
go test -v ./ottrec/...
```

#### Running the scraper locally using cached data

```bash
# optional: reset the cache and data to the latest upstream version
git -C cache clean -fdx
git -C data clean -fdx
git -C cache reset --hard HEAD
git -C data reset --hard HEAD
git -C cache pull
git -C data pull

# run the scraper
go run ./ottrec/scraper -cache ./cache -geocodio -scrape -export.pretty -export.proto ./data/data.proto -export.pb ./data/data.pb -export.textpb ./data/data.textpb -export.json ./data/data.json

# inspect the changes
git -C data diff
```

#### Updating the schema

Also run this when updating the protobuf module.

- Do not make backwards-incompatible protobuf changes.
- Avoid renaming fields unless absolutely necessary (this will break JSON users).
- Avoid adding fields which do not mirror the inherent structure of the website and could be consistently computed from the existing fields.
- Underscored fields can be used for computed fields where how they are parsed may change in the future, but the field itself is an inherent property (e.g., schedule date ranges from the caption).
- Do not change the semantic meaning of existing fields.
- Do not remove fields entirely; deprecate them, but continue to set them.
- Keep fields in sync with the website ottrecidx package.

If backwards-incompatible changes are ever necessary, create a new v2 subdir with the new schema, put stuff in there, create a new v2 api using the v2 schema, and create a new v2 branch in the data repo.

```bash
buf lint ./ottrec/schema/schema.proto # ignore the warnings about underscored field names, v1 dir, and the weekday enum
buf breaking --against ./data/data.proto ./ottrec/schema/schema.proto
go generate ./ottrec/schema
```

### Website

#### Running it locally with automatic restart

```bash
export DEBUG_POSTCSS_NOOP=1 # optional: don't process stylesheets with postcss
env -C website watchexec --clear --debounce 1s -f '*.templ' --watch ./templates 'go generate ./templates'
env -C website watchexec --clear --debounce 1s -i '*.templ' --restart 'go run ./cmd/ottrec-data' # http://data.ottrec.localhost:8082/
env -C website watchexec --clear --debounce 1s -i '*.templ' --restart 'go run ./cmd/ottrec-website' # http://ottrec.localhost:8083/
```

#### Updating fonts and JS libs

```bash
go generate ./website/static
```

#### Running ottrecidx sanity checks

```bash
go run ./website/pkg/ottrecidx/profile.go -check
```

#### Profiling ottrecidx

```bash
go run ./pkg/ottrecidx/profile.go -cpuprofile /tmp/cpu.pprof -memprofile /tmp/mem.pprof
go tool pprof -http :6060 /tmp/cpu.pprof
go tool pprof -http :6061 /tmp/mem.pprof
```
