import pytest
import os
from datetime import datetime

# Arguments for pytest.main
# - 'tests/' specifies the directory of the tests
# - '--html=html/report.html' tells pytest to generate an HTML report
# - '--self-contained-html' creates a single HTML file including all assets
args = ['tests/', f'--html={os.getcwd()}/tests/reports/airlines_{datetime.now().strftime("%Y%m%d%H%M%s")}.html', '--self-contained-html']

if __name__ == "__main__":
    
    # Run pytest and get the exit code
    exit_code = pytest.main(args)

    # Optionally, you can use the exit code for further logic
    print(f"Pytest exited with code {exit_code}")
