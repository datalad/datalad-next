python -m coverage xml
curl -fsSL -o codecov.exe "https://uploader.codecov.io/latest/windows/codecov.exe"
.\codecov.exe -f "coverage.xml"
