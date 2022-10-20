# BEAM INTRO

## Create module
```
go mod init github.com/magal1337/beam-series/beam-series-go/beam_intro
```

## install beam go sdk
```
go get -u github.com/apache/beam/sdks/v2/go/pkg/beam
```

## install all main.go dependencies
```
go mod tidy
```

## run Beam Job
```
go run main.go --output counts.txt
```