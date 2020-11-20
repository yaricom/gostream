#
# Go parameters
#
GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test -count=1
GOGET = $(GOCMD) get
BINARY_NAME = gostream
BINARY_UNIX = $(BINARY_NAME)_unix
OUT_DIR = out
#
# Environment services setup
#
REDIS_ADDR=127.0.0.1:6379

#
# The debug flag
#
DEBUG =

# The default targets to run
#
all: test build

# Builds binary
#
build: | $(OUT_DIR)
	$(GOBUILD) -o $(OUT_DIR)/$(BINARY_NAME) -v

# Creates the output directory for build artefacts
#
$(OUT_DIR):
	mkdir -p $@

# Run all tests including integration
#
# Usage make test DEBUG=true REDIS_ADDR=127.0.0.1:6379
test:
	DEBUG=$(DEBUG) ENV_REDIS_TEST_HOSTS=$(REDIS_ADDR) $(GOTEST) -v ./...

#
# Clean build targets
#
clean: 
	$(GOCLEAN)
	rm -f $(OUT_DIR)/$(BINARY_NAME)
	rm -f $(OUT_DIR)/$(BINARY_UNIX)

# Helper allowing printing values of the variables defined in the Makefile
# Example: make print-PROJECT_ID
print-%: ; @echo $*=$($*)

#
# Cross compilation
#
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(OUT_DIR)/$(BINARY_UNIX) -v

# Check that given variables are set and all have non-empty values,
# die with an error otherwise.
#
# Params:
#   1. Variable name(s) to test.
#   2. (optional) Error message to print.
check_defined = \
    $(strip $(foreach 1,$1, \
        $(call __check_defined,$1,$(strip $(value 2)))))
__check_defined = \
    $(if $(value $1),, \
        $(error Undefined $1$(if $2, ($2))$(if $(value @), required by target '$@')))

# Creates topic or subscription ID by concatenating values of given variables.
#
# Params:
#	1. The variable for topic/subscription name
#	2. The variable with suffix
create_id = $(value $(strip $1))-$(value $(strip $2))