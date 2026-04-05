#!/usr/bin/env bash

# Create a new organization-specific CSV splitter from template
# This script generates a boilerplate splitter class that you can customize

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Usage check
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./add-csv-splitter.sh <org-id> <class-name>"
    echo ""
    echo "Example: ./add-csv-splitter.sh community-health CommunityHealth"
    echo "         Creates: splitters/community_health.py with CommunityHealthSplitter class"
    exit 1
fi

ORG_ID=$1
CLASS_NAME=$2

# Convert org-id to valid Python module name (replace hyphens with underscores)
MODULE_NAME=$(echo "$ORG_ID" | tr '-' '_')
SPLITTER_DIR="lambda/multi-org/csv-splitter/splitters"
OUTPUT_FILE="${SPLITTER_DIR}/${MODULE_NAME}.py"

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Creating CSV Splitter                                ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Organization ID:${NC} $ORG_ID"
echo -e "${BLUE}Class Name:${NC} ${CLASS_NAME}Splitter"
echo -e "${BLUE}Output File:${NC} $OUTPUT_FILE"
echo ""

# Check if file already exists
if [ -f "$OUTPUT_FILE" ]; then
    echo -e "${YELLOW}Warning: File already exists: $OUTPUT_FILE${NC}"
    read -p "Overwrite? (y/N): " OVERWRITE
    if [ "$OVERWRITE" != "y" ]; then
        echo "Exiting."
        exit 1
    fi
fi

# Create the splitter file
cat > "$OUTPUT_FILE" << EOF
"""${CLASS_NAME} CSV splitter for ${ORG_ID}."""

import csv
from io import StringIO
from typing import List, Tuple

from .base_splitter import BaseCsvSplitter


class ${CLASS_NAME}Splitter(BaseCsvSplitter):
    """
    ${CLASS_NAME} CSV splitter.

    TODO: Customize this class for your organization's CSV format.

    Key things to implement:
    1. Set the ID_COLUMN to the column containing the chart/visit ID
    2. Implement split() to separate bulk CSV into individual charts
    3. Add any filtering logic (by date, program, status, etc.)
    """

    @property
    def org_id(self) -> str:
        return "${ORG_ID}"

    # TODO: Set this to the column name or index containing the chart ID
    ID_COLUMN = "chart_id"  # Change this to match your CSV

    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Args:
            csv_content: Raw CSV file content as string
            filename: Original filename for reference

        Returns:
            List of tuples: (chart_id, csv_content)
            Each chart_id becomes the output filename: chart_{chart_id}.csv

        TODO: Implement your splitting logic here. Common patterns:

        1. One row = one chart (like Catholic Charities):
           - Read with csv.DictReader
           - Filter rows by criteria (date, status, program)
           - Output each row as individual CSV

        2. Multiple rows per chart (like Circles of Care):
           - Group rows by ID column
           - Output each group as individual CSV

        3. Custom format:
           - Parse headers
           - Apply org-specific logic
        """
        results = []

        try:
            reader = csv.DictReader(StringIO(csv_content))
            headers = reader.fieldnames

            if not headers:
                print(f"WARNING: No headers found in {filename}")
                return results

            for row in reader:
                # TODO: Add filtering logic here
                # Example: Skip rows that don't meet criteria
                # if row.get('status') != 'active':
                #     continue

                # Get chart ID from the ID column
                chart_id = row.get(self.ID_COLUMN, '').strip()
                if not chart_id:
                    print(f"WARNING: No {self.ID_COLUMN} found in row")
                    continue

                # Create individual CSV for this chart
                output = StringIO()
                writer = csv.DictWriter(output, fieldnames=headers)
                writer.writeheader()
                writer.writerow(row)

                results.append((chart_id, output.getvalue()))

        except Exception as e:
            print(f"ERROR splitting CSV {filename}: {e}")

        return results
EOF

echo -e "${GREEN}✓${NC} Created splitter: $OUTPUT_FILE"

# Update __init__.py to include the new splitter
INIT_FILE="${SPLITTER_DIR}/__init__.py"

echo ""
echo -e "${BLUE}Updating ${INIT_FILE}${NC}"

# Check if import already exists
if grep -q "from .${MODULE_NAME}" "$INIT_FILE" 2>/dev/null; then
    echo -e "${YELLOW}⚠${NC} Import already exists in __init__.py"
else
    # Add import and __all__ entry
    python3 - <<EOF
import re

init_file = '${INIT_FILE}'
module_name = '${MODULE_NAME}'
class_name = '${CLASS_NAME}Splitter'

with open(init_file, 'r') as f:
    content = f.read()

# Add import line after existing imports
import_line = f"from .{module_name} import {class_name}"
if import_line not in content:
    # Find the last import line and add after it
    lines = content.split('\n')
    last_import_idx = 0
    for i, line in enumerate(lines):
        if line.startswith('from .'):
            last_import_idx = i

    lines.insert(last_import_idx + 1, import_line)
    content = '\n'.join(lines)

# Add to __all__ list
if class_name not in content:
    content = re.sub(
        r"(__all__ = \[[\s\S]*?)(])",
        rf"\1    '{class_name}',\n\2",
        content
    )

with open(init_file, 'w') as f:
    f.write(content)

print(f"✓ Added {class_name} to {init_file}")
EOF
fi

# Update the Lambda handler to import the new splitter
HANDLER_FILE="lambda/multi-org/csv-splitter/csv_splitter_multi_org.py"

echo ""
echo -e "${BLUE}Updating ${HANDLER_FILE}${NC}"

if grep -q "from splitters.${MODULE_NAME}" "$HANDLER_FILE" 2>/dev/null; then
    echo -e "${YELLOW}⚠${NC} Import already exists in handler"
else
    python3 - <<EOF
module_name = '${MODULE_NAME}'
class_name = '${CLASS_NAME}Splitter'
handler_file = '${HANDLER_FILE}'

with open(handler_file, 'r') as f:
    content = f.read()

# Add import after existing splitter imports
import_line = f"from splitters.{module_name} import {class_name}"
if import_line not in content:
    # Find last splitter import
    lines = content.split('\n')
    last_import_idx = 0
    for i, line in enumerate(lines):
        if 'from splitters.' in line and 'import' in line:
            last_import_idx = i

    if last_import_idx > 0:
        lines.insert(last_import_idx + 1, import_line)
        content = '\n'.join(lines)

# Add to splitter_classes list in register_splitters()
if class_name not in content:
    content = content.replace(
        'splitter_classes = [',
        f'splitter_classes = [\n        {class_name},'
    )

with open(handler_file, 'w') as f:
    f.write(content)

print(f"✓ Added {class_name} to handler")
EOF
fi

# Summary
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   CSV Splitter Created!                                ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Files Modified:${NC}"
echo "  Created: $OUTPUT_FILE"
echo "  Updated: $INIT_FILE"
echo "  Updated: $HANDLER_FILE"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Edit $OUTPUT_FILE and customize:"
echo "   - Set ID_COLUMN to your chart ID column name"
echo "   - Implement split() with your filtering/grouping logic"
echo ""
echo "2. Configure CSV column mappings in Admin UI:"
echo "   - Go to Organization > Field Mappings tab"
echo "   - Add csv_column_mappings for field extraction"
echo ""
echo "3. Deploy the updated Lambda:"
echo "   cd infra && cdk deploy PenguinHealthCsvSplitterStack"
echo ""
echo "4. Configure S3 trigger:"
echo "   ./scripts/multi-org/add-csv-splitter-trigger.sh $ORG_ID"
echo ""
