package com.fooddelivery.kitchen.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ensures the target database exists before HikariCP and Flyway attempt to connect.
 *
 * Spring Boot's DataSource autoconfiguration and Flyway both run after BeanFactoryPostProcessors,
 * so this is the earliest safe hook to bootstrap the database. The processor connects to the
 * built-in "postgres" system database (always present), checks pg_database for the target DB,
 * and creates it if missing. Flyway then handles schema creation (create-schemas: true) and
 * all table DDL via migrations — no manual SQL scripts required.
 */
@Configuration
public class DatabaseBootstrapConfig {

    @Bean
    public static DatabaseEnsurePostProcessor databaseEnsurePostProcessor() {
        return new DatabaseEnsurePostProcessor();
    }

    static class DatabaseEnsurePostProcessor implements BeanFactoryPostProcessor, EnvironmentAware {

        private static final Logger log = LoggerFactory.getLogger(DatabaseEnsurePostProcessor.class);
        private static final Pattern JDBC_URL_PATTERN =
                Pattern.compile("jdbc:postgresql://([^/?]+)/([^?]+)");

        private Environment environment;

        @Override
        public void setEnvironment(Environment environment) {
            this.environment = environment;
        }

        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
                throws BeansException {

            String url      = environment.getProperty("spring.datasource.url", "");
            String username = environment.getProperty("spring.datasource.username", "postgres");
            String password = environment.getProperty("spring.datasource.password", "");

            Matcher matcher = JDBC_URL_PATTERN.matcher(url);
            if (!matcher.find()) {
                log.warn("Cannot parse JDBC URL '{}' — skipping database bootstrap", url);
                return;
            }

            String hostPort  = matcher.group(1);  // e.g. localhost:5432
            String dbName    = matcher.group(2);  // e.g. fooddelivery
            // Connect to the always-present postgres system database to run DDL
            String adminUrl  = "jdbc:postgresql://" + hostPort + "/postgres";

            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("PostgreSQL JDBC driver not found", e);
            }

            try (Connection conn = DriverManager.getConnection(adminUrl, username, password);
                 Statement  stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                        "SELECT 1 FROM pg_database WHERE datname = '" + dbName + "'");

                if (!rs.next()) {
                    stmt.execute("CREATE DATABASE \"" + dbName + "\"");
                    log.info("Bootstrap: created database '{}'", dbName);
                } else {
                    log.debug("Bootstrap: database '{}' already exists", dbName);
                }

            } catch (Exception e) {
                log.error("Bootstrap: failed to ensure database '{}' exists: {}", dbName,
                        e.getMessage());
                throw new RuntimeException(
                        "Database bootstrap failed — cannot create '" + dbName + "'", e);
            }
        }
    }
}
