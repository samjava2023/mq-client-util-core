package com.db.pwmus.mqclient.ibmmq;

import com.db.pwmus.mqclient.api.MessageFormat;
import com.db.pwmus.mqclient.api.MqMessage;
import com.db.pwmus.mqclient.config.ConnectionConfig;
import com.db.pwmus.mqclient.config.QueueConfig;
import com.db.pwmus.mqclient.support.MqFlowLog;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class IbmMqJmsSupport {
    private static final String CLASS = "IbmMqJmsSupport";

    /**
     * Oracle/JSSE cipher-suite names for common IBM MQ CipherSpecs when
     * {@code useIbmCipherMappings} is {@code false} (non-IBM JRE mapping table).
     */
    private static final Map<String, String> MQ_CIPHER_TO_TLS_CIPHER;
    static {
        Map<String, String> map = new HashMap<String, String>();
        map.put("ECDHE_RSA_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        map.put("ECDHE_RSA_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        map.put("ECDHE_RSA_AES_256_CBC_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");
        map.put("ECDHE_RSA_AES_128_CBC_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
        map.put("ECDHE_ECDSA_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        map.put("ECDHE_ECDSA_AES_128_GCM_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        map.put("TLS_RSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_256_GCM_SHA384");
        map.put("TLS_RSA_WITH_AES_128_GCM_SHA256", "TLS_RSA_WITH_AES_128_GCM_SHA256");
        MQ_CIPHER_TO_TLS_CIPHER = Collections.unmodifiableMap(map);
    }

    private IbmMqJmsSupport() {
    }

    static void send(ConnectionConfig connectionConfig,
                   QueueConfig queueConfig,
                   String body,
                   MessageFormat format,
                   Map<String, String> headers) {
        String physicalQueue = queueConfig.getResolvedName();
        MqFlowLog.debug(CLASS, "send", "opening JMS connection | physicalQueue=" + physicalQueue
            + ", contentType=" + format.contentType()
            + " | " + MqFlowLog.formatConnection(null, connectionConfig));
        Connection connection = null;
        Session session = null;
        try {
            MQQueueConnectionFactory factory = createConnectionFactory(connectionConfig);
            connection = openConnection(factory, connectionConfig);
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(physicalQueue);
            MessageProducer producer = session.createProducer(queue);

            TextMessage message = session.createTextMessage(body);
            message.setStringProperty("contentType", format.contentType());
            applyHeaders(message, headers);

            connection.start();
            MqFlowLog.debug(CLASS, "send", "producer.send | physicalQueue=" + physicalQueue
                + ", bodyLength=" + (body == null ? 0 : body.length())
                + ", headers=" + MqFlowLog.formatHeaders(headers));
            producer.send(message);
            MqFlowLog.debug(CLASS, "send", "producer.send completed | physicalQueue=" + physicalQueue);
        } catch (JMSException e) {
            MqFlowLog.fail(CLASS, "send", "physicalQueue=" + physicalQueue, e);
            throw new IllegalStateException("IBM MQ send failed for queue " + physicalQueue, e);
        } finally {
            closeQuietly(session);
            closeQuietly(connection);
        }
    }

    static MqMessage receive(ConnectionConfig connectionConfig,
                             QueueConfig queueConfig,
                             long timeout,
                             TimeUnit unit) {
        String physicalQueue = queueConfig.getResolvedName();
        MqFlowLog.debug(CLASS, "receive", "opening JMS connection | physicalQueue=" + physicalQueue
            + ", timeout=" + timeout + ", unit=" + unit
            + " | " + MqFlowLog.formatConnection(null, connectionConfig));
        Connection connection = null;
        Session session = null;
        try {
            MQQueueConnectionFactory factory = createConnectionFactory(connectionConfig);
            connection = openConnection(factory, connectionConfig);
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(physicalQueue);
            MessageConsumer consumer = session.createConsumer(queue);

            connection.start();
            long timeoutMs = unit.toMillis(timeout);
            MqFlowLog.debug(CLASS, "receive", "consumer.receive | physicalQueue=" + physicalQueue
                + ", timeoutMs=" + timeoutMs);
            Message message = consumer.receive(timeoutMs);
            if (message == null) {
                MqFlowLog.debug(CLASS, "receive", "no message | physicalQueue=" + physicalQueue);
                return null;
            }
            MqMessage mqMessage = toMqMessage(message);
            MqFlowLog.debug(CLASS, "receive", "message received | physicalQueue=" + physicalQueue
                + ", contentType=" + mqMessage.getContentType()
                + ", bodyLength=" + (mqMessage.getBody() == null ? 0 : mqMessage.getBody().length()));
            return mqMessage;
        } catch (JMSException e) {
            MqFlowLog.fail(CLASS, "receive", "physicalQueue=" + physicalQueue, e);
            throw new IllegalStateException("IBM MQ receive failed for queue " + physicalQueue, e);
        } finally {
            closeQuietly(session);
            closeQuietly(connection);
        }
    }

    private static MQQueueConnectionFactory createConnectionFactory(ConnectionConfig config) throws JMSException {
        if (config.getHost() == null || config.getPort() == null) {
            throw new IllegalArgumentException(
                "host/port or connectionName is required for IBM MQ (resolved host="
                    + config.getHost() + ", port=" + config.getPort()
                    + ", connectionName=" + config.getConnectionName() + ")");
        }
        if (config.getQueueManager() == null) {
            throw new IllegalArgumentException("queueManager is required for IBM MQ");
        }
        if (config.getChannel() == null) {
            throw new IllegalArgumentException("channel is required for IBM MQ");
        }

        applySslStores(config);
        configureIbmCipherMappings(config);
        configurePreferTls(config);

        MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
        factory.setHostName(config.getHost());
        factory.setPort(config.getPort());
        factory.setQueueManager(config.getQueueManager());
        factory.setChannel(config.getChannel());
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);

        String connectionName = trimToNull(config.getConnectionName());
        if (connectionName != null) {
            factory.setStringProperty("WMQ_CONNECTION_NAME", connectionName);
        }

        configureAuthentication(factory, config);
        configureSsl(factory, config);

        if (config.getConnectionTimeoutMs() != null) {
            factory.setIntProperty(WMQConstants.WMQ_CLIENT_RECONNECT_TIMEOUT, config.getConnectionTimeoutMs());
        }

        MqFlowLog.debug(CLASS, "createConnectionFactory", "mode=" + describeConnectionMode(config)
            + " | " + MqFlowLog.formatConnection(null, config));

        return factory;
    }

    private static void configureAuthentication(MQQueueConnectionFactory factory, ConnectionConfig config)
            throws JMSException {
        String username = trimToNull(config.getUsername());
        if (username != null) {
            factory.setStringProperty(WMQConstants.USERID, username);
            if (config.getPassword() != null) {
                factory.setStringProperty(WMQConstants.PASSWORD, config.getPassword());
            }
        }

        Boolean useCsp = config.getUseMqCspAuthentication();
        if (useCsp != null && !useCsp.booleanValue()) {
            System.setProperty("com.ibm.mq.cfg.jmqi.useMQCSPauthentication", "N");
            factory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, false);
        }
    }

    private static void configureSsl(MQQueueConnectionFactory factory, ConnectionConfig config) throws JMSException {
        if (!isSslEnabled(config)) {
            return;
        }

        String configuredCipher = trimToNull(config.getSslCipherSpec());
        if (configuredCipher == null) {
            throw new IllegalArgumentException(
                "sslCipherSpec is required when ssl is true (e.g. ECDHE_RSA_AES_256_GCM_SHA384)");
        }

        String cipher = resolveSslCipherSuite(configuredCipher, config);
        factory.setSSLCipherSuite(cipher);
        MqFlowLog.debug(CLASS, "configureSsl", "enabled cipherSuite=" + cipher
            + (cipher.equals(configuredCipher) ? "" : " (from sslCipherSpec=" + configuredCipher + ")")
            + ", auth=" + (hasCredentials(config) ? "username/password" : "none")
            + ", useIbmCipherMappings=" + config.getUseIbmCipherMappings()
            + ", preferTls=" + resolvePreferTls(config));
    }

    /**
     * When {@code useIbmCipherMappings} is {@code false}, IBM MQ expects a JSSE cipher suite name
     * (e.g. {@code TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384}), not the queue-manager CipherSpec
     * ({@code ECDHE_RSA_AES_256_GCM_SHA384}). Production configs often use the MQ name; translate here.
     */
    private static String resolveSslCipherSuite(String configuredCipher, ConnectionConfig config) {
        Boolean useMappings = config.getUseIbmCipherMappings();
        if (useMappings != null && !useMappings.booleanValue()) {
            if (configuredCipher.startsWith("TLS_") || configuredCipher.startsWith("SSL_")) {
                return configuredCipher;
            }
            String tlsCipher = MQ_CIPHER_TO_TLS_CIPHER.get(configuredCipher.toUpperCase(Locale.US));
            if (tlsCipher != null) {
                return tlsCipher;
            }
        }
        return configuredCipher;
    }

    /**
     * When {@code ssl} is {@code true}, prefer TLS for the IBM MQ client connection
     * ({@code com.ibm.mq.cfg.preferTLS=true}) unless {@code preferTls} is explicitly {@code false}.
     */
    private static void configurePreferTls(ConnectionConfig config) {
        if (!isSslEnabled(config)) {
            return;
        }
        boolean preferTls = resolvePreferTls(config);
        System.setProperty("com.ibm.mq.cfg.preferTLS", preferTls ? "true" : "false");
        MqFlowLog.debug(CLASS, "configurePreferTls",
            "com.ibm.mq.cfg.preferTLS=" + preferTls);
    }

    private static boolean resolvePreferTls(ConnectionConfig config) {
        Boolean preferTls = config.getPreferTls();
        return preferTls == null || preferTls.booleanValue();
    }

    /**
     * When {@code useIbmCipherMappings} is {@code false}, switch IBM MQ to Oracle/JSSE cipher mappings
     * (required on non-IBM JREs for TLS 1.2 suites). Ignored on IBM MQ 9.3.3+ but harmless.
     */
    private static void configureIbmCipherMappings(ConnectionConfig config) {
        Boolean useMappings = config.getUseIbmCipherMappings();
        if (useMappings != null && !useMappings.booleanValue()) {
            System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false");
            MqFlowLog.debug(CLASS, "configureIbmCipherMappings",
                "com.ibm.mq.cfg.useIBMCipherMappings=false");
        }
    }

    private static void applySslStores(ConnectionConfig config) {
        if (!isSslEnabled(config)) {
            return;
        }

        String trustStore = trimToNull(config.getSslTrustStore());
        if (trustStore != null) {
            System.setProperty("javax.net.ssl.trustStore", trustStore);
            String trustStorePassword = config.getSslTrustStorePassword();
            if (trustStorePassword != null) {
                System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
            }
        }

        String keyStore = trimToNull(config.getSslKeyStore());
        if (keyStore != null) {
            System.setProperty("javax.net.ssl.keyStore", keyStore);
            String keyStorePassword = config.getSslKeyStorePassword();
            if (keyStorePassword != null) {
                System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
            }
        }
    }

    private static Connection openConnection(MQQueueConnectionFactory factory, ConnectionConfig config)
            throws JMSException {
        String username = trimToNull(config.getUsername());
        if (username != null) {
            String password = config.getPassword() != null ? config.getPassword() : "";
            return factory.createConnection(username, password);
        }
        return factory.createConnection();
    }

    private static boolean isSslEnabled(ConnectionConfig config) {
        return Boolean.TRUE.equals(config.getSsl());
    }

    private static boolean hasCredentials(ConnectionConfig config) {
        return trimToNull(config.getUsername()) != null;
    }

    private static String describeConnectionMode(ConnectionConfig config) {
        String transport = isSslEnabled(config) ? "ssl" : "plain";
        String auth = hasCredentials(config) ? "userpass" : "no-auth";
        return transport + "+" + auth;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static void applyHeaders(TextMessage message, Map<String, String> headers) throws JMSException {
        if (headers == null) {
            return;
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            message.setStringProperty(entry.getKey(), entry.getValue());
        }
    }

    private static MqMessage toMqMessage(Message message) throws JMSException {
        String body = "";
        if (message instanceof TextMessage) {
            body = ((TextMessage) message).getText();
        }

        String contentType = message.getStringProperty("contentType");
        Map<String, String> headers = new HashMap<String, String>();
        if (contentType != null) {
            headers.put("contentType", contentType);
        }

        try {
            String correlationId = message.getJMSCorrelationID();
            if (correlationId != null) {
                headers.put("correlationId", correlationId);
            }
        } catch (JMSException ignore) {
            // ignore
        }

        return new MqMessage(body, contentType, headers);
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException ignore) {
                // ignore
            }
        }
    }

    private static void closeQuietly(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException ignore) {
                // ignore
            }
        }
    }
}
