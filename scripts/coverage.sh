#!/bin/bash

# ════════════════════════════════════════════════════════════
# Test Coverage Runner for Amulet Scan
# ════════════════════════════════════════════════════════════
#
# Generates test coverage reports across all test suites
#
# Usage: ./scripts/coverage.sh [suite]
#
# Suites:
#   all       - Run all coverage (default)
#   frontend  - Frontend component coverage (vitest)
#   server    - Server API coverage
#   ingest    - Ingest script coverage
#   summary   - Show coverage summary only
#
# ════════════════════════════════════════════════════════════

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Coverage output directory
COVERAGE_DIR="$PROJECT_ROOT/coverage"

# Default suite
SUITE="${1:-all}"

print_header() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}  $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BLUE}────────────────────────────────────────────────────────────${NC}"
    echo -e "${BOLD}  $1${NC}"
    echo -e "${BLUE}────────────────────────────────────────────────────────────${NC}"
}

# Create coverage directory
mkdir -p "$COVERAGE_DIR"

# ────────────────────────────────────────────────────────────
# Frontend Coverage (Vitest)
# ────────────────────────────────────────────────────────────
run_frontend_coverage() {
    print_section "📦 Frontend Coverage (Vitest)"
    
    cd "$PROJECT_ROOT"
    
    # Check if vitest is available
    if ! npm list vitest > /dev/null 2>&1; then
        echo -e "${YELLOW}⚠ Vitest not found, skipping frontend coverage${NC}"
        return 0
    fi
    
    # Run vitest with coverage (disable thresholds for reporting)
    # Ensure json-summary is produced (coverage-summary.json)
    echo "Running frontend tests with coverage..."
    npx vitest run --coverage \
        --coverage.reporter=text --coverage.reporter=json --coverage.reporter=json-summary --coverage.reporter=html \
        --coverage.reportsDirectory="$COVERAGE_DIR/frontend" \
        --coverage.thresholds.statements=0 \
        --coverage.thresholds.branches=0 \
        --coverage.thresholds.functions=0 \
        --coverage.thresholds.lines=0 2>&1 || true
    
    if [ -f "$COVERAGE_DIR/frontend/coverage-summary.json" ]; then
        echo -e "${GREEN}✅ Frontend coverage report generated${NC}"
    else
        echo -e "${YELLOW}⚠ Frontend coverage report not generated${NC}"
    fi
}

# ────────────────────────────────────────────────────────────
# Server Coverage
# ────────────────────────────────────────────────────────────
run_server_coverage() {
    print_section "🖥️  Server Coverage"
    
    cd "$PROJECT_ROOT"
    
    echo "Running server tests with coverage via vitest..."
    
    # Note: vitest config already includes server tests; we run the whole suite
    # but restrict coverage collection to server/** so the report is server-only.
    timeout 180 npx vitest run \
        --coverage \
        --coverage.reporter=text --coverage.reporter=json --coverage.reporter=json-summary --coverage.reporter=html \
        --coverage.reportsDirectory="$COVERAGE_DIR/server" \
        --coverage.include="server/lib/**/*.js" \
        --coverage.include="server/api/**/*.js" \
        --coverage.include="server/engine/**/*.js" \
        --coverage.include="server/duckdb/**/*.js" \
        --coverage.exclude="src/**" \
        --coverage.thresholds.statements=0 \
        --coverage.thresholds.branches=0 \
        --coverage.thresholds.functions=0 \
        --coverage.thresholds.lines=0 \
        2>&1 || true
    
    if [ -f "$COVERAGE_DIR/server/coverage-summary.json" ]; then
        echo -e "${GREEN}✅ Server coverage report generated${NC}"
    else
        echo -e "${YELLOW}⚠ Server coverage report not generated${NC}"
    fi
}

# ────────────────────────────────────────────────────────────
# Ingest Coverage
# ────────────────────────────────────────────────────────────
run_ingest_coverage() {
    print_section "📥 Ingest Script Coverage"
    
    cd "$PROJECT_ROOT/scripts/ingest"
    
    # Check if dependencies are installed
    if [ ! -d "node_modules" ]; then
        echo "Installing ingest dependencies..."
        npm install
    fi
    
    # Check if c8 is available, install if not
    if ! npx c8 --version > /dev/null 2>&1; then
        echo "Installing c8 for coverage..."
        npm install --save-dev c8
    fi
    
    echo "Running ingest tests with coverage..."
    echo "(timeout: 60s - API tests only)"
    
    # Run with shorter timeout since API tests can hang on network issues
    # Use set +e to prevent script exit on timeout
    set +e
    timeout 60 npx c8 \
        --reporter=text \
        --reporter=json-summary \
        --reporter=html \
        --reports-dir="$COVERAGE_DIR/ingest" \
        --include="*.js" \
        --exclude="node_modules/**" \
        node test/api.test.js 2>&1
    
    exit_code=$?
    set -e
    
    if [ $exit_code -eq 124 ]; then
        echo -e "${YELLOW}⚠ Ingest tests timed out (network issues?)${NC}"
    elif [ $exit_code -ne 0 ]; then
        echo -e "${YELLOW}⚠ Ingest tests exited with code $exit_code${NC}"
    fi
    
    if [ -f "$COVERAGE_DIR/ingest/coverage-summary.json" ]; then
        echo -e "${GREEN}✅ Ingest coverage report generated${NC}"
    else
        echo -e "${YELLOW}⚠ Ingest coverage report not generated${NC}"
    fi
}

# ────────────────────────────────────────────────────────────
# Generate Summary
# ────────────────────────────────────────────────────────────
generate_summary() {
    print_section "📊 Coverage Summary"
    
    # Create summary file
    SUMMARY_FILE="$COVERAGE_DIR/summary.txt"
    
    echo "════════════════════════════════════════════════════════════" > "$SUMMARY_FILE"
    echo "  AMULET SCAN TEST COVERAGE SUMMARY" >> "$SUMMARY_FILE"
    echo "  Generated: $(date)" >> "$SUMMARY_FILE"
    echo "════════════════════════════════════════════════════════════" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"
    
    # Parse coverage from each suite
    total_lines=0
    covered_lines=0
    
    for suite in frontend server ingest; do
        summary_json="$COVERAGE_DIR/$suite/coverage-summary.json"
        if [ -f "$summary_json" ]; then
            # Extract total coverage percentage
            pct=$(node -e "
                const data = require('$summary_json');
                const total = data.total || {};
                const lines = total.lines || { pct: 0 };
                console.log(lines.pct.toFixed(2));
            " 2>/dev/null || echo "N/A")
            
            lines_total=$(node -e "
                const data = require('$summary_json');
                console.log(data.total?.lines?.total || 0);
            " 2>/dev/null || echo "0")
            
            lines_covered=$(node -e "
                const data = require('$summary_json');
                console.log(data.total?.lines?.covered || 0);
            " 2>/dev/null || echo "0")
            
            total_lines=$((total_lines + lines_total))
            covered_lines=$((covered_lines + lines_covered))
            
            printf "%-15s %s%%\n" "$suite:" "$pct" >> "$SUMMARY_FILE"
            echo -e "  ${BOLD}$suite:${NC} ${GREEN}$pct%${NC}"
        else
            printf "%-15s %s\n" "$suite:" "N/A" >> "$SUMMARY_FILE"
            echo -e "  ${BOLD}$suite:${NC} ${YELLOW}N/A${NC}"
        fi
    done
    
    echo "" >> "$SUMMARY_FILE"
    
    # Calculate overall coverage
    if [ $total_lines -gt 0 ]; then
        overall=$(echo "scale=2; $covered_lines * 100 / $total_lines" | bc 2>/dev/null || echo "N/A")
        echo "────────────────────────────────────────────────────────────" >> "$SUMMARY_FILE"
        printf "%-15s %s%%\n" "OVERALL:" "$overall" >> "$SUMMARY_FILE"
        echo "" >> "$SUMMARY_FILE"
        echo -e "\n  ${BOLD}OVERALL:${NC} ${CYAN}$overall%${NC} ($covered_lines/$total_lines lines)"
    fi
    
    echo "" >> "$SUMMARY_FILE"
    echo "Reports available at:" >> "$SUMMARY_FILE"
    echo "  - $COVERAGE_DIR/frontend/index.html" >> "$SUMMARY_FILE"
    echo "  - $COVERAGE_DIR/server/index.html" >> "$SUMMARY_FILE"
    echo "  - $COVERAGE_DIR/ingest/index.html" >> "$SUMMARY_FILE"
    
    echo ""
    echo -e "${BLUE}📁 Reports available at:${NC}"
    echo "   $COVERAGE_DIR/frontend/index.html"
    echo "   $COVERAGE_DIR/server/index.html"
    echo "   $COVERAGE_DIR/ingest/index.html"
    echo ""
    echo -e "${GREEN}📄 Summary saved to: $SUMMARY_FILE${NC}"
}

# ────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────

print_header "🧪 Amulet Scan Test Coverage"

case "$SUITE" in
    frontend)
        run_frontend_coverage
        ;;
    server)
        run_server_coverage
        ;;
    ingest)
        run_ingest_coverage
        ;;
    summary)
        generate_summary
        ;;
    all|*)
        run_frontend_coverage
        run_server_coverage
        run_ingest_coverage
        generate_summary
        ;;
esac

print_header "✅ Coverage Complete"

echo -e "
${BOLD}Usage:${NC}
  ./scripts/coverage.sh              # Run all coverage
  ./scripts/coverage.sh frontend     # Frontend only
  ./scripts/coverage.sh server       # Server only
  ./scripts/coverage.sh ingest       # Ingest only
  ./scripts/coverage.sh summary      # Show summary only

${BOLD}View HTML Reports:${NC}
  open $COVERAGE_DIR/frontend/index.html
  open $COVERAGE_DIR/server/index.html
  open $COVERAGE_DIR/ingest/index.html
"
