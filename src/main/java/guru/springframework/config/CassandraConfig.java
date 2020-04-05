package guru.springframework.config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.*;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jt on 10/6/17.
 * edit Konstantin (kmwork)
 */
@Configuration
@ConfigurationProperties(prefix = "cassandra")
@EnableCassandraRepositories(basePackages = "guru.springframework.repositories")
@Slf4j
public class CassandraConfig extends AbstractCassandraConfiguration {

    @Value("${cassandra.keyspace-name}")
    protected String keyspaceName;

    @Value("${cassandra.port}")
    protected Integer cassandraPort;

    @Value("${cassandra.contact-points}")
    protected String cassandraPoints;

    @Override
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

    @Override
    protected String getKeyspaceName() {
        return keyspaceName;
    }

    @Override
    protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
        CreateKeyspaceSpecification specification = CreateKeyspaceSpecification.createKeyspace(keyspaceName);

        return Arrays.asList(specification);
    }

    @Override
    protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
        return Arrays.asList(DropKeyspaceSpecification.dropKeyspace(keyspaceName));
    }


    @Override
    public String[] getEntityBasePackages() {
        return new String[]{"guru.springframework.domain"};
    }


    @Override
    @Bean
    public CassandraClusterFactoryBean cluster() {
        final CassandraClusterFactoryBean cluster = new CassandraClusterFactoryBean();
        cluster.setContactPoints(cassandraPoints);
        cluster.setPort(cassandraPort);
        cluster.setJmxReportingEnabled(false);
        log.info("Cluster created with contact points = {} & port = {}", cassandraPoints, cassandraPort);
        return cluster;
    }

    @Override
    @Bean
    public CassandraMappingContext cassandraMapping() throws ClassNotFoundException {
        CassandraMappingContext bean = new CassandraMappingContext();
        bean.setInitialEntitySet(CassandraEntityClassScanner.scan(("guru.springframework.domain")));

        return bean;
    }

    @Bean
    public CassandraConverter converter() throws ClassNotFoundException {
        return new MappingCassandraConverter(cassandraMapping());
    }

    @SneakyThrows
    @Override
    @Bean
    public CassandraSessionFactoryBean session() {

        CassandraSessionFactoryBean session = new CassandraSessionFactoryBean();
        session.setCluster(cluster().getObject());
        session.setKeyspaceName(keyspaceName);
        session.setConverter(converter());
        session.setSchemaAction(SchemaAction.CREATE);

        return session;
    }

}
