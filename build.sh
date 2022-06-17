#!/bin/bash

rm zinc

cd web
# npm install
# npm run build
cd ..

export VERSION=`git describe --tags --always`
export BUILD_DATE=`date -u '+%Y-%m-%d_%I:%M:%S%p-GMT'`
export COMMIT_HASH=`git rev-parse HEAD`

export ZINC_LDFLAGS="-w -s -X github.com/zinclabs/zinc/pkg/meta.Version=${VERSION} -X github.com/zinclabs/zinc/pkg/meta.BuildDate=${BUILD_DATE} -X github.com/zinclabs/zinc/pkg/meta.CommitHash=${COMMIT_HASH}"

# CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s -X main.Version=$VERSION -X main.Date=$DATE -X main.Commit=$COMMIT_HASH" -o zinc cmd/zinc/main.go
# CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags="-X main.Version=$VERSION -X main.Date=$DATE -X main.Commit=$COMMIT_HASH" -o zinc cmd/zinc/main.go
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags="$ZINC_LDFLAGS" -o zinc cmd/zinc/main.go
