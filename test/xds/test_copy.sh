cat test/xds/xds_client_sni_test.go | sed -n '324,477p' > test/xds/test_body.go
cat test/xds/test_body.go | sed 's/TestClientSideXDS_FallbackSANMatchers/TestClientSideXDS_SNIDisabledSANMatchers/' > test/xds/test_body_new.go
cat test/xds/test_body_new.go | sed 's/testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, true)/testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, false)/' > test/xds/test_body_final.go
cat test/xds/test_body_final.go >> test/xds/xds_client_sni_test.go
