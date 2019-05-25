"dos2unix.exe" "social-network-analysis/src/main/java/ch/ethz/infk/dspa/config.properties"
"dos2unix.exe" "social-network-analysis/docker-entrypoint.sh"
"dos2unix.exe" "stream-producer/wait-for.sh"
"dos2unix.exe" "web/wait-for.sh"
"dos2unix.exe" "scripts/_start.sh"

for /f "tokens=* delims=" %%a in ('dir "data/1k-users-sorted/tables" /s /b') do (
"dos2unix.exe" %%a
)

for /f "tokens=* delims=" %%a in ('dir "data/10k-users-sorted/tables" /s /b') do (
"dos2unix.exe" %%a
)

for /f "tokens=* delims=" %%a in ('dir "data/schema" /s /b') do (
"dos2unix.exe" %%a
)
