// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use base64::encode_config;
use base64::URL_SAFE_NO_PAD;
use common_base::base::tokio;
use common_exception::Result;
use common_meta_types::UserIdentity;
use common_users::CustomClaims;
use common_users::EnsureUser;
use common_users::UserApiProvider;
use databend_query::auth::Credential;
use databend_query::sessions::TableContext;
use jwt_simple::prelude::*;
use p256::EncodedPoint;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_with_jwt() -> Result<()> {
    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = encode_config(rsa_components.e, URL_SAFE_NO_PAD);
    let n = encode_config(rsa_components.n, URL_SAFE_NO_PAD);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;
    let jwks_url = format!("http://{}{}", server.address(), json_path);

    let mut conf = crate::tests::ConfigBuilder::create().config();
    conf.query.jwt_key_file = jwks_url.clone();
    let (_guard, ctx) = crate::tests::create_query_context_with_config(conf, None).await?;
    let auth_mgr = ctx.get_auth_manager();
    let tenant = "test";
    let user_name = "test";

    // without subject
    {
        let claims = Claims::create(Duration::from_hours(2));
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 1051, displayText = missing field `subject` in jwt.",
            res.err().unwrap().to_string()
        );
    }

    // without custom claims
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    // with custom claims
    {
        let custom_claims = CustomClaims::new();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    // with create user
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser::default());
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await?;
        let user_info = ctx.get_current_user()?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user again
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string()]),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await?;
        let user_info = ctx.get_current_user()?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user and grant roles
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec![role_name.to_string()]),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_ok());

        let user_info = UserApiProvider::instance()
            .get_user(tenant, UserIdentity::new(user_name, "%"))
            .await?;
        assert_eq!(user_info.grants.roles().len(), 1);
        assert_eq!(user_info.grants.roles()[0], role_name.to_string());
    }

    // with create user and auth role
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
            })
            .with_role("test-auth-role");
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_ok());

        let roles: Vec<String> = ctx
            .get_current_session()
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(roles.len(), 1);
        assert!(!roles.contains(&"test-auth-role".to_string()));
    }

    // root auth from localhost
    {
        let user_name = "root";

        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: Some("localhost".to_string()),
            })
            .await;
        assert!(res.is_ok());
    }

    // root auth outside localhost
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject("root".to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: Some("10.0.0.1".to_string()),
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = only accept root from localhost, current: 'root'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_with_jwt_es256() -> Result<()> {
    let kid = "test_kid";
    let key_pair = ES256KeyPair::generate().with_key_id(kid);
    let encoded_point =
        EncodedPoint::from_bytes(key_pair.public_key().public_key().to_bytes_uncompressed())
            .expect("must be valid encode point");
    let x = encode_config(encoded_point.x().unwrap(), URL_SAFE_NO_PAD);
    let y = encode_config(encoded_point.y().unwrap(), URL_SAFE_NO_PAD);
    let j =
        serde_json::json!({"keys": [ {"kty": "EC", "kid": kid, "x": x, "y": y, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;
    let jwks_url = format!("http://{}{}", server.address(), json_path);

    let mut conf = crate::tests::ConfigBuilder::create().config();
    conf.query.jwt_key_file = jwks_url.clone();
    let (_guard, ctx) = crate::tests::create_query_context_with_config(conf, None).await?;
    let auth_mgr = ctx.get_auth_manager();
    let tenant = "test";
    let user_name = "test";

    // without subject
    {
        let claims = Claims::create(Duration::from_hours(2));
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 1051, displayText = missing field `subject` in jwt.",
            res.err().unwrap().to_string()
        );
    }

    // without custom claims
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    // with custom claims
    {
        let custom_claims = CustomClaims::new();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    // with create user
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser::default());
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await?;
        let user_info = ctx.get_current_user()?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user again
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string()]),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await?;
        let user_info = ctx.get_current_user()?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user and grant roles
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec![role_name.to_string()]),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_ok());

        let user_info = UserApiProvider::instance()
            .get_user(tenant, UserIdentity::new(user_name, "%"))
            .await?;
        assert_eq!(user_info.grants.roles().len(), 1);
        assert_eq!(user_info.grants.roles()[0], role_name.to_string());
    }

    // with create user and auth role
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
            })
            .with_role("test-auth-role");
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await;
        assert!(res.is_ok());

        let roles: Vec<String> = ctx
            .get_current_session()
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(roles.len(), 1);
        assert!(!roles.contains(&"test-auth-role".to_string()));
    }

    // root auth from localhost
    {
        let user_name = "root";

        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: Some("localhost".to_string()),
            })
            .await;
        assert!(res.is_ok());
    }

    // root auth outside localhost
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject("root".to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: Some("10.0.0.1".to_string()),
            })
            .await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = only accept root from localhost, current: 'root'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_jwt_auth_mgr_with_management() -> Result<()> {
    let kid = "test_kid";
    let user_name = "test";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = encode_config(rsa_components.e, URL_SAFE_NO_PAD);
    let n = encode_config(rsa_components.n, URL_SAFE_NO_PAD);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;

    let mut conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();
    conf.query.jwt_key_file = format!("http://{}{}", server.address(), json_path);
    let (_guard, ctx) = crate::tests::create_query_context_with_config(conf, None).await?;
    let auth_mgr = ctx.get_auth_manager();

    // with create user in other tenant
    {
        let tenant = "other";
        let custom_claims = CustomClaims::new()
            .with_tenant_id(tenant)
            .with_ensure_user(EnsureUser::default());
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(ctx.get_current_session(), &Credential::Jwt {
                token,
                hostname: None,
            })
            .await?;
        let user_info = ctx.get_current_user()?;
        let current_tenant = ctx.get_tenant();
        assert_eq!(current_tenant, tenant.to_string());
        assert_eq!(user_info.grants.roles().len(), 0);

        Ok(())
    }
}
