:: set -x
@echo on
:: download and ingest docker image
curl -fsSL --ssl-no-revoke -o sshd.dockerimg.gz %DATALAD_TESTS_DOCKER_SSHD_DOWNLOADURL%
gzip -c -d sshd.dockerimg.gz | docker load
:: start container
docker run --rm -dit --name %DATALAD_TESTS_DOCKER_SSHD_CONTAINER_NAME% -p %DATALAD_TESTS_SERVER_SSH_PORT%:22 -v %DATALAD_TESTS_SERVER_LOCALPATH%:%DATALAD_TESTS_SERVER_SSH_PATH% sshd
:: give the service a moment to start (otherwise we may run into timeouts on windows)
sleep 10
:: wipe any other known host keys
ssh-keygen -f C:\Users\appveyor\.ssh\known_hosts -R "[%DATALAD_TESTS_SERVER_SSH_HOST%]:%DATALAD_TESTS_SERVER_SSH_PORT%"
:: ingest actual host key
ssh-keyscan -t ecdsa -p %DATALAD_TESTS_SERVER_SSH_PORT% %DATALAD_TESTS_SERVER_SSH_HOST% >> C:\Users\appveyor\.ssh\known_hosts
:: get the ssh key matching the container
curl -fsSL --ssl-no-revoke -o %DATALAD_TESTS_SERVER_SSH_SECKEY% %DATALAD_TESTS_DOCKER_SSHD_SECKEY_DOWNLOADURL%
:: establish expected permission setup for SSH key
tools\appveyor\chmod600.bat %DATALAD_TESTS_SERVER_SSH_SECKEY%
