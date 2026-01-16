#!/bin/bash
# Unified test runner - works from any directory
# Usage: ./scripts/run-tests.sh [test-type]
#
# Test types:
#   all       - Run all ingest tests (default)
#   api       - Test Scan API connectivity
#   backfill  - Test backfill data integrity
#   acs       - Test ACS snapshot integrity
#   gcs       - Test GCS upload integrity
#   preflight - Run GCS preflight checks
#   validate  - Run all validation scripts
#   server    - Run server tests
#   frontend  - Run frontend tests
#   full      - Run everything (ingest + server + frontend)

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INGEST_DIR="$PROJECT_ROOT/scripts/ingest"
SERVER_DIR="$PROJECT_ROOT/server"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

run_ingest_test() {
    local test_name=$1
    print_header "Running Ingest Test: $test_name"
    cd "$INGEST_DIR"
    
    # Check if node_modules exists
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm run "test:$test_name"
}

run_ingest_tests() {
    print_header "Running All Ingest Tests"
    cd "$INGEST_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm test
}

run_server_tests() {
    print_header "Running Server Tests"
    cd "$SERVER_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing server dependencies..."
        npm install
    fi
    
    npm test
}

run_frontend_tests() {
    print_header "Running Frontend Tests"
    cd "$PROJECT_ROOT"
    npm test
}

run_preflight() {
    print_header "Running GCS Preflight Checks"
    cd "$INGEST_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm run preflight
}

run_validation() {
    print_header "Running Validation Scripts"
    cd "$INGEST_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm run validate
}

show_help() {
    echo "Unified Test Runner for Amulet Scan"
    echo ""
    echo "Usage: $0 [test-type]"
    echo ""
    echo "Test types:"
    echo "  all        Run all ingest tests (api, backfill, acs) [default]"
    echo "  api        Test Scan API connectivity and data structure"
    echo "  backfill   Test backfill data integrity"
    echo "  acs        Test ACS snapshot integrity"
    echo "  gcs        Test GCS upload integrity (requires GCS_BUCKET)"
    echo "  health     Run end-to-end pipeline health check"
    echo "  preflight  Run GCS preflight checks"
    echo "  validate   Run all validation scripts"
    echo "  server     Run server API tests"
    echo "  frontend   Run frontend component tests"
    echo "  full       Run everything (ingest + server + frontend)"
    echo ""
echo "Coverage:"
    echo "  coverage   Run all tests with coverage reports"
    echo "  cov:front  Frontend coverage only"
    echo "  cov:server Server coverage only"
    echo "  cov:ingest Ingest coverage only"
    echo ""
    echo "Quality & Mutation:"
    echo "  quality    Generate test quality report"
    echo "  mutate     Run Stryker mutation testing"
    echo "  mutate:dry Dry run mutation testing (show what would be mutated)"
    echo ""
    echo "Examples:"
    echo "  $0              # Run all ingest tests"
    echo "  $0 api          # Test API connectivity only"
    echo "  $0 health       # Run full pipeline health check"
    echo "  $0 coverage     # Generate coverage reports"
    echo "  $0 quality      # Generate test quality report"
    echo "  $0 mutate       # Run mutation testing"
    echo "  $0 full         # Run all test suites"
    echo ""
    echo "Environment variables:"
    echo "  GCS_BUCKET      Required for gcs, health, and preflight tests"
    echo "  SCAN_URL        Scan API URL (default: https://scan.sv-1.global.canton.network.sync.global)"
    echo ""
}

# Main
TEST_TYPE="${1:-all}"

case "$TEST_TYPE" in
    help|--help|-h)
        show_help
        ;;
    all)
        run_ingest_tests
        print_success "All ingest tests passed!"
        ;;
    api)
        run_ingest_test "api"
        print_success "API tests passed!"
        ;;
    backfill)
        run_ingest_test "backfill"
        print_success "Backfill tests passed!"
        ;;
    acs)
        run_ingest_test "acs"
        print_success "ACS tests passed!"
        ;;
    gcs)
        run_ingest_test "gcs"
        print_success "GCS integrity tests passed!"
        ;;
    health)
        run_ingest_test "health"
        print_success "Pipeline health check passed!"
        ;;
    health:quick)
        run_ingest_test "health:quick"
        print_success "Quick health check passed!"
        ;;
    preflight)
        run_preflight
        print_success "Preflight checks passed!"
        ;;
    validate)
        run_validation
        print_success "Validation passed!"
        ;;
    server)
        run_server_tests
        print_success "Server tests passed!"
        ;;
    frontend)
        run_frontend_tests
        print_success "Frontend tests passed!"
        ;;
    full)
        print_header "Running Full Test Suite"
        
        # Ingest tests
        run_ingest_tests
        print_success "Ingest tests passed!"
        
        # Server tests
        run_server_tests
        print_success "Server tests passed!"
        
        # Frontend tests
        run_frontend_tests
        print_success "Frontend tests passed!"
        
        echo ""
        print_success "All test suites passed!"
        ;;
    coverage)
        print_header "Running All Tests with Coverage"
        bash "$SCRIPT_DIR/coverage.sh" all
        ;;
    cov:front|cov:frontend)
        print_header "Running Frontend Coverage"
        bash "$SCRIPT_DIR/coverage.sh" frontend
        ;;
    cov:server)
        print_header "Running Server Coverage"
        bash "$SCRIPT_DIR/coverage.sh" server
        ;;
    cov:ingest)
        print_header "Running Ingest Coverage"
        bash "$SCRIPT_DIR/coverage.sh" ingest
        ;;
    quality)
        print_header "Generating Test Quality Report"
        cd "$PROJECT_ROOT"
        node scripts/test-quality-report.js
        ;;
    mutate)
        print_header "Running Stryker Mutation Testing"
        cd "$PROJECT_ROOT"
        npx stryker run
        ;;
    mutate:dry)
        print_header "Stryker Dry Run (showing mutants)"
        cd "$PROJECT_ROOT"
        npx stryker run --dryRun
        ;;
    *)
        print_error "Unknown test type: $TEST_TYPE"
        echo ""
        show_help
        exit 1
        ;;
esac

echo ""
print_info "Tests completed from: $(pwd)"
