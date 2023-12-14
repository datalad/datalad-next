set key=%1
:: remove inheritance
icacls %key% /c /t /Inheritance:d
:: set ownership to owner
icacls %key% /c /t /Grant %UserName%:F
:: remove all users except owner
icacls %key% /c /t /Remove:g "Authenticated Users" BUILTIN\Administrators BUILTIN Everyone System Users
:: cleanup
set "key="
