#!/usr/bin/env bash

set -o errexit
set -o nounset
# set -o xtrace

# Positional arguments:
#
# 1. kopia_robustness_dir
# 2. kopia_exe_dir
# 3. test_duration
# 4. test_timeout
# 5. test_repo_path_prefix

# Environment variables that modify the behavior of the robustness job execution
#
# - AWS_ACCESS_KEY_ID: To access the repo bucket
# - AWS_SECRET_ACCESS_KEY: To access the repo bucket
# - ENGINE_MODE:
# - FIO_EXE: Path to the fio executable, if unset a Docker container will be
#       used to run fio.
# - HOST_FIO_DATA_PATH:
# - LOCAL_FIO_DATA_PATH: Path to the local directory where snapshots should be
#       restored to and fio data should be written to
# - S3_BUCKET_NAME: Name of the S3 bucket for the repo

readonly kopia_recovery_dir="${1?Specify directory with kopia robustness git repo}"
readonly kopia_exe_dir="${2?Specify the directory of the kopia git repo to be tested}"

readonly test_duration=${3:?"Provide a minimum duration for the testing, e.g., '15m'"}
readonly test_timeout=${4:?"Provide a timeout for the test run, e.g., '55m'"}
readonly test_repo_path_prefix=${5:?"Provide the path that contains the data and metadata repos"}

# Remaining arguments are additional optional test flags
shift 5

cat <<EOF
--- Job parameters ----
kopia_recovery_dir: '${kopia_recovery_dir}'
kopia_exe_dir: '${kopia_exe_dir}'
test_duration: '${test_duration}'
test_timeout: '${test_timeout}'
test_repo_path_prefix: '${test_repo_path_prefix}'
additional_args: '${@}'

--- Optional Job Parameters via Environment Variables ---
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:+<xxxx>}
ENGINE_MODE=${ENGINE_MODE-}
FIO_EXE=${FIO_EXE-}
HOST_FIO_DATA_PATH:${HOST_FIO_DATA_PATH-}
LOCAL_FIO_DATA_PATH=${LOCAL_FIO_DATA_PATH-}
S3_BUCKET_NAME=${S3_BUCKET_NAME-}
TEST_RC=${TEST_RC-}

--- Other Env Vars ---
GOBIN=${GOBIN-}
GOPATH=${GOPATH-}

---

EOF

if [ -n "${LOCAL_FIO_DATA_PATH-}" ] ; then
    echo "Contents of data dir: '${LOCAL_FIO_DATA_PATH}'"
    ls -oF "${LOCAL_FIO_DATA_PATH}"

    echo "Storage used on: '${LOCAL_FIO_DATA_PATH}'"
    df -h "${LOCAL_FIO_DATA_PATH}"
fi

readonly kopia_exe="${kopia_exe_dir}/kopia"

# Extract git metadata from the exe repo and build kopia
pushd "${kopia_exe_dir}"

readonly kopia_git_revision=$(git rev-parse --short HEAD)
readonly kopia_git_branch="$(git describe --tags --always --dirty)"
readonly kopia_git_dirty=$(git diff-index --quiet HEAD -- || echo "*")
readonly kopia_build_time=$(date +%FT%T%z)

go build -o "${kopia_exe}" github.com/kopia/kopia

popd

# Extract git metadata on the recovery repo, perform a robustness run
pushd "${kopia_recovery_dir}"

readonly robustness_git_revision=$(git rev-parse --short HEAD)
readonly robustness_git_branch="$(git describe --tags --always --dirty)"
readonly robustness_git_dirty=$(git diff-index --quiet HEAD -- || echo "*")
readonly robustness_build_time=$(date +%FT%T%z)

readonly ld_flags="\
-X github.com/kopia/kopia/tests/robustness/engine.repoBuildTime=${kopia_build_time} \
-X github.com/kopia/kopia/tests/robustness/engine.repoGitRevision=${kopia_git_dirty:-""}${kopia_git_revision} \
-X github.com/kopia/kopia/tests/robustness/engine.repoGitBranch=${kopia_git_branch} \
-X github.com/kopia/kopia/tests/robustness/engine.testBuildTime=${robustness_build_time} \
-X github.com/kopia/kopia/tests/robustness/engine.testGitRevision=${robustness_git_dirty:-""}${robustness_git_revision} \
-X github.com/kopia/kopia/tests/robustness/engine.testGitBranch=${robustness_git_branch}"

readonly test_flags="-v -timeout=${test_timeout}\
 --rand-test-duration=${test_duration}\
 --repo-path-prefix=${test_repo_path_prefix}\
 -ldflags '${ld_flags}'"

# Set the make target based on ENGINE_MODE
ENGINE_MODE="${ENGINE_MODE:-}"
if [[ "${ENGINE_MODE}" == "" ]]; then
    make_target="recovery-tests"
else
    make_target="robustness-tests"
fi
if [[ "${ENGINE_MODE}" = SERVER ]]; then
    make_target="robustness-server-tests"
fi

# Source any pre-test rc files if provided
TEST_RC="${TEST_RC:-}"
if [[ -f ${TEST_RC} ]]; then
    source ${TEST_RC}
fi

# Run the robustness tests
set -o verbose

make -C "${kopia_recovery_dir}" \
    KOPIA_EXE="${kopia_exe}" \
    GO_TEST='go test' \
    TEST_FLAGS="${test_flags}" \
    "${make_target}"

popd
