package com.jonathanfoucher.kafkastream.config;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class EnvConfig {
    private static final String GROUP_ID = "GROUP_ID";
    private static final String BOOTSTRAP_SERVER = "BOOTSTRAP_SERVER";
    private static final String TOPIC_IN = "TOPIC_IN";
    private static final String TOPIC_OUT = "TOPIC_OUT";
    private static final String SCHEMA_REGISTRY_URL = "SCHEMA_REGISTRY_URL";
    private static final String SCHEMA_REGISTRY_USER = "SCHEMA_REGISTRY_USER";
    private static final String SCHEMA_REGISTRY_PASSWORD = "SCHEMA_REGISTRY_PASSWORD";
    private static final String AUTH_SOURCE = "AUTH_SOURCE";

    private static final String SECURITY_PROTOCOL = "SECURITY_PROTOCOL";
    private static final String SSL_PROTOCOL = "SSL_PROTOCOL";
    private static final String SSL_KEYSTORE_TYPE = "SSL_KEYSTORE_TYPE";
    private static final String SSL_KEYSTORE_KEY = "SSL_KEYSTORE_KEY";
    private static final String SSL_KEYSTORE_CERT = "SSL_KEYSTORE_CERT";
    private static final String SSL_TRUSTSTORE_TYPE = "SSL_TRUSTSTORE_TYPE";
    private static final String SSL_TRUSTSTORE_CERT = "SSL_TRUSTSTORE_CERT";

    private static final String HTTP_SERVER_PORT = "HTTP_SERVER_PORT";

    public String getGroupId() {
        return System.getenv(GROUP_ID);
    }

    public String getBootstrapServer() {
        return System.getenv(BOOTSTRAP_SERVER);
    }

    public String getTopicIn() {
        return System.getenv(TOPIC_IN);
    }

    public String getTopicOut() {
        return System.getenv(TOPIC_OUT);
    }

    public String getSchemaRegistryUrl() {
        return System.getenv(SCHEMA_REGISTRY_URL);
    }

    public String getAuthSource() {
        return System.getenv(AUTH_SOURCE);
    }

    public String getSecurityProtocol() {
        return System.getenv(SECURITY_PROTOCOL);
    }

    public String getSslProtocol() {
        return System.getenv(SSL_PROTOCOL);
    }

    public String getSslKeystoreType() {
        return System.getenv(SSL_KEYSTORE_TYPE);
    }

    public String getSslKeystoreKey() {
        return System.getenv(SSL_KEYSTORE_KEY);
    }

    public String getSslKeystoreCert() {
        return System.getenv(SSL_KEYSTORE_CERT);
    }

    public String getSslTruststoreType() {
        return System.getenv(SSL_TRUSTSTORE_TYPE);
    }

    public String getSslTruststoreCert() {
        return System.getenv(SSL_TRUSTSTORE_CERT);
    }

    public int getHttpServerPort() {
        return Integer.parseInt(System.getenv(HTTP_SERVER_PORT));
    }

    public String getUserInfo() {
        String user = System.getenv(SCHEMA_REGISTRY_USER);
        String password = System.getenv(SCHEMA_REGISTRY_PASSWORD);
        return user + ":" + password;
    }

    public String getAutoRegisterSchemas() {
        return String.valueOf(Boolean.FALSE);
    }
}
