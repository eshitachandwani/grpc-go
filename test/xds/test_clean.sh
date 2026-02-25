sed -i '' '479,483d' test/xds/xds_client_sni_test.go
sed -i '' 's/testutils.SetEnvConfig(t, testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, true)envconfig.XDSSNIEnabled, false)/testutils.SetEnvConfig(t, \&envconfig.XDSSNIEnabled, false)/' test/xds/xds_client_sni_test.go
