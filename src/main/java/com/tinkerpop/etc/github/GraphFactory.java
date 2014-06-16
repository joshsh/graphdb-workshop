package com.tinkerpop.etc.github;

import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.etc.github.beans.ActorAttributes;
import com.tinkerpop.etc.github.beans.Comment;
import com.tinkerpop.etc.github.beans.Event;
import com.tinkerpop.etc.github.beans.Links;
import com.tinkerpop.etc.github.beans.Milestone;
import com.tinkerpop.etc.github.beans.Page;
import com.tinkerpop.etc.github.beans.Payload;
import com.tinkerpop.etc.github.beans.Permissions;
import com.tinkerpop.etc.github.beans.PullRequest;
import com.tinkerpop.etc.github.beans.Release;
import com.tinkerpop.etc.github.beans.Repository;
import com.tinkerpop.etc.github.beans.RepositoryBrief;
import com.tinkerpop.etc.github.beans.Revision;
import com.tinkerpop.etc.github.beans.Team;
import com.tinkerpop.etc.github.beans.Urls;
import com.tinkerpop.etc.github.beans.User;
import com.tinkerpop.etc.github.beans.Webpage;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.lang.reflect.Field;
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

        return createTitanGraph(conf, keyspace);
    }

    public static TitanGraph createTitanOnCassandra(final String host,
                                                    final String keyspace) {
        Configuration conf = new BaseConfiguration();

        conf.setProperty("storage.backend", "cassandra");
        conf.setProperty("storage.hostname", host);

        return createTitanGraph(conf, keyspace);
    }

    public static TitanGraph createTitanOnHBase(final String keyspace) {
        Configuration conf = new BaseConfiguration();

        conf.setProperty("storage.backend", "hbase");

        return createTitanGraph(conf, keyspace);
    }

    private static TitanGraph createTitanGraph(final Configuration conf,
                                               final String keyspace) {
        conf.setProperty("storage.batch-loading", "true");
        conf.setProperty("set-vertex-id", "true");
        conf.setProperty("autotype", "none");

        // conservatively estimate 1500 events/s, disregarding other vertices
        conf.setProperty("ids.block-size", "5400000");

        if (null != keyspace) {
            conf.setProperty("storage.keyspace", keyspace);
        }

        TitanGraph g = TitanFactory.open(conf);

        TitanType timestamp;

        if (null == g.getType(IdGraph.ID)) {
            g.makeKey(IdGraph.ID).single().unique().indexed(Vertex.class).dataType(String.class).make();
        }

        // special keys
        if (null == (timestamp = g.getType(GithubSchema.TIMESTAMP))) {
            timestamp = g.makeKey(GithubSchema.TIMESTAMP).dataType(Long.class).make();
        }
        if (null == g.getType(GithubSchema.TYPE)) {
            g.makeKey(GithubSchema.TYPE).dataType(String.class).make();
        }

        // labels (add them before property keys)
        for (GithubSchema.Label label : GithubSchema.Label.values()) {
            if (null == g.getType(label.name())) {
                LOGGER.info("making type for label: " + label.name());
                g.makeLabel(label.name()).sortKey(timestamp).sortOrder(Order.DESC).make();
            }
        }

        // vertex keys
        for (Class clazz : new Class[]{ActorAttributes.class, Comment.class, Event.class, Links.class, Milestone.class,
                Page.class, Payload.class, Permissions.class, PullRequest.class, Release.class, Repository.class,
                RepositoryBrief.class, Revision.class, Team.class, Urls.class, User.class, Webpage.class}) {

            for (Field f : clazz.getFields()) {
                if (EventHandler.PROPERTY_CLASSES.contains(f.getType())) {
                    String key = f.getName();
                    key = EventHandler.fixPropertyKey(key);
                    if (null == g.getType(key)) {
                        LOGGER.info("making type for property: " + key);
                        g.makeKey(key).dataType(f.getType()).make();
                    }
                }
            }
        }

        g.commit();

        return g;
    }
}
