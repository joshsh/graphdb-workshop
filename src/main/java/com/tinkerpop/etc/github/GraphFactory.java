package com.tinkerpop.etc.github;

import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.logging.Logger;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphFactory {
    protected static final Logger LOGGER = Logger.getLogger(GraphFactory.class.getName());

    public static TinkerGraph createTinkerGraph() {
        return new TinkerGraph();
    }

    public static TitanGraph createTitanOnBerkeleyJE(final String dir,
                                                     final String keyspace) {
        Configuration conf = new BaseConfiguration();

        conf.setProperty("storage.backend", "berkeleyje");
        conf.setProperty("storage.directory", dir);
        conf.setProperty("storage.batch-loading", "true");
        if (null != keyspace) {
            conf.setProperty("storage.keyspace", keyspace);
        }

        TitanGraph g = TitanFactory.open(conf);

        setupTitanGraph(g);

        return g;
    }

    public static TitanGraph createTitanOnCassandra(final String host,
                                                    final String keyspace) {
        Configuration conf = new BaseConfiguration();

        conf.setProperty("storage.backend", "cassandra");
        conf.setProperty("storage.hostname", host);
        conf.setProperty("storage.batch-loading", "true");
        if (null != keyspace) {
            conf.setProperty("storage.keyspace", keyspace);
        }

        TitanGraph g = TitanFactory.open(conf);

        setupTitanGraph(g);

        return g;
    }

    public static TitanGraph createTitanOnHBase(final String keyspace) {
        Configuration conf = new BaseConfiguration();

        conf.setProperty("storage.backend", "hbase");
        conf.setProperty("storage.batch-loading", "true");
        if (null != keyspace) {
            conf.setProperty("storage.keyspace", keyspace);
        }

        TitanGraph g =  TitanFactory.open(conf);

        setupTitanGraph(g);

        return g;
    }

    private static void setupTitanGraph(final TitanGraph g) {

        TitanType createdAt;

        if (null == g.getType(IdGraph.ID)) {
            g.makeKey(IdGraph.ID).single().unique().indexed(Vertex.class).indexed(Edge.class).dataType(String.class).make();
        }

        if (null == g.getType("name")) {
            g.createKeyIndex("name", Vertex.class);
        }

        // edge-specific keys
        for (String key : new String[]{"github_type", "payload", "public", "url"}) {
            if (null == g.getType(key)) {
                g.makeKey(key).dataType(String.class).make();
            }
        }

        // vertex keys
        for (String key : new String[]{"action", "actor", "blog", "comment", "comment_id", "commit", "company", "description", "email", "fork", "forks", "github_id", "github_name", "github_type", "gravatar_id", "has_downloads", "has_issues", "has_wiki", "homepage", "issue", "language", "location", "login", "master_branch", "name", "number", "open_issues", "organization", "owner", "page_name", "payload", "private", "pushed_at", "sha", "size", "stargazers", "title", "type", "url", "watchers"}) {
            if (null == g.getType(key)) {
                g.makeKey(key).dataType(String.class).make();
            }
        }

        // special keys
        if (null == (createdAt = g.getType("created_at"))) {
            createdAt = g.makeKey("created_at").dataType(Long.class).make();
        }

        // labels
        for (String label : new String[]{"added", "created", "deleted", "edited", "forked", "of", "on", "owns", "pullRequested", "pushed", "to", "watched"}) {
            if (null == g.getType(label)) {
                g.makeLabel(label).sortKey(createdAt).sortOrder(Order.DESC).make();
            }
        }

        g.commit();
    }
}
