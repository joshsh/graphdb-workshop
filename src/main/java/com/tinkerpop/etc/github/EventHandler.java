package com.tinkerpop.etc.github;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.etc.github.beans.Comment;
import com.tinkerpop.etc.github.beans.Event;
import com.tinkerpop.etc.github.beans.Page;
import com.tinkerpop.etc.github.beans.PullRequest;
import com.tinkerpop.etc.github.beans.Release;
import com.tinkerpop.etc.github.beans.RepositoryBrief;
import com.tinkerpop.etc.github.beans.Team;
import com.tinkerpop.etc.github.beans.User;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class EventHandler {
    protected static final Logger LOGGER = Logger.getLogger(EventHandler.class.getName());

    private static final DateFormat
            OLD_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z"), // e.g. 2012/03/11 00:00:00 -0800
            NEW_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"); // e.g. 2014-05-31T00:13:30-07:00

    public static final Set<Class> PROPERTY_CLASSES;

    static {
        PROPERTY_CLASSES = new HashSet<Class>();
        PROPERTY_CLASSES.add(String.class);
        PROPERTY_CLASSES.add(Boolean.class);
        PROPERTY_CLASSES.add(Long.class);
        PROPERTY_CLASSES.add(Integer.class);
    }

    private final Graph graph;
    private final HashFunction idHashFunction;

    public EventHandler(final Graph graph) {
        this.graph = graph;

        idHashFunction = Hashing.sipHash24();
    }

    public void handle(final Event sourceEvent) throws IllegalAccessException {

        long timestamp = parseTimestamp(sourceEvent.created_at);

        Vertex eventV = getOrCreateVertex(randomVertexId(), null, GithubSchema.TYPE_EVENT);
        //event.setProperty(GithubSchema.EVENT_TYPE, eventType.name());

        setProperties(sourceEvent, eventV);

        if (null != sourceEvent.actor) {
            Vertex actor = getActor(sourceEvent);
            addEdgeTo(eventV, actor, GithubSchema.Label.actor, timestamp);
        }

        if (null != sourceEvent.repository) {
            Vertex repo = getRepository(sourceEvent.repository);
            addEdgeTo(eventV, repo, GithubSchema.Label.repository, timestamp);
        }

        if (null != sourceEvent.payload) {
            setProperties(sourceEvent.payload, eventV);

            if (null != sourceEvent.payload.target) {
                Vertex user = getUser(sourceEvent.payload.target);
                addEdgeTo(eventV, user, GithubSchema.Label.target, timestamp);
            }

            if (null != sourceEvent.payload.pages) {
                for (int i = 0; i < sourceEvent.payload.pages.length; i++) {
                    Vertex page = getPage(sourceEvent.payload.pages[i]);
                    addEdgeTo(eventV, page, GithubSchema.Label.page, timestamp);
                }
            }

            if (null != sourceEvent.payload.member) {
                Vertex user = getUser(sourceEvent.payload.member);
                addEdgeTo(eventV, user, GithubSchema.Label.member, timestamp);
            }

            if (null != sourceEvent.payload.pull_request) {
                Vertex user = getPullRequest(sourceEvent.payload.pull_request);
                addEdgeTo(eventV, user, GithubSchema.Label.pull_request, timestamp);
            }

            if (null != sourceEvent.payload.comment) {
                Vertex comment = getComment(sourceEvent.payload.comment);
                addEdgeTo(eventV, comment, GithubSchema.Label.comment, timestamp);
            }

            if (null != sourceEvent.payload.shas) {
                int index = 0;
                for (Object sha : sourceEvent.payload.shas) {
                    if (PROPERTY_CLASSES.contains(sha.getClass())) {
                        index++;
                        eventV.setProperty("sha_" + index, sha);
                    }
                }
            }

            if (null != sourceEvent.payload.release) {
                addEdgeTo(eventV, getRelease(sourceEvent.payload.release), GithubSchema.Label.release, timestamp);
            }

            if (null != sourceEvent.payload.team) {
                Vertex team = getTeam(sourceEvent.payload.team);
                addEdgeTo(eventV, team, GithubSchema.Label.team, timestamp);
            }
        }
    }

    private final Random random = new Random();

    private final IDInspector idInspector = new IDManager().getIDInspector();

    private long randomVertexId() {
        long id;
        do {
            id = Math.abs(idHashFunction.hashLong(random.nextLong()).asLong());
        } while (!idInspector.isVertexID(id));

        return id;
    }

    private long randomEdgeId() {
        long id;
        do {
            id = Math.abs(idHashFunction.hashLong(random.nextLong()).asLong());
        } while (!idInspector.isRelationID(id));

        return id;
    }

    private long hashedVertexId(final String toHash) {
        long id = Math.abs(idHashFunction.hashString(toHash, Charset.defaultCharset()).asLong());
        int count = 0;
        while (!idInspector.isVertexID(id)) {
            if (++count == 64) {
                throw new IllegalStateException("couldn't find a valid ID");
            }
            id = Math.abs(id * 2);
        }

        return id;
    }

    protected long parseTimestamp(final String timestamp) {
        Date d;

        try {
            d = NEW_TIMESTAMP_FORMAT.parse(timestamp);
        } catch (ParseException e) {
            try {
                d = OLD_TIMESTAMP_FORMAT.parse(timestamp);
            } catch (ParseException e1) {
                throw new IllegalArgumentException("not a valid timestamp: " + timestamp);
            }
        }

        return d.getTime();
    }

    protected void addEdgeTo(final Vertex eventV,
                             final Vertex inV,
                             final GithubSchema.Label label,
                             final long timestamp) {
        Edge e = graph.addEdge(randomEdgeId(), eventV, inV, label.name());
        e.setProperty(GithubSchema.TIMESTAMP, timestamp);
    }

    protected Vertex getOrCreateVertex(final long internalId,
                                       final String originalId,
                                       final String type) {
        Vertex v = graph.getVertex(internalId);
        if (null == v) {
            v = graph.addVertex(internalId);
            if (null != originalId) {
                v.setProperty(IdGraph.ID, originalId);
            }
            v.setProperty(GithubSchema.TYPE, type);
        }

        return v;
    }

    private Vertex getVertex(final Object source,
                             final Object id,
                             final String prefix,
                             final String type) throws IllegalAccessException {

        String originalId = null == id ? null : prefix + id;
        long internalId = null == id ? randomVertexId() : hashedVertexId(originalId);

        Vertex v = getOrCreateVertex(internalId, originalId, type);
        if (null != source) {
            setProperties(source, v);
        }
        return v;
    }

    protected Vertex getActor(final Event source) throws IllegalAccessException {
        return getVertex(source.actor_attributes, source.actor, "user:", GithubSchema.TYPE_USER);
    }

    protected Vertex getUser(final User source) throws IllegalAccessException {
        return getVertex(source, source.login, "user:", GithubSchema.TYPE_USER);
    }

    protected Vertex getPage(final Page source) throws IllegalAccessException {
        return getVertex(source, source.sha, "page:", GithubSchema.TYPE_PAGE);
    }

    protected Vertex getPullRequest(final PullRequest source) throws IllegalAccessException {
        return getVertex(source, source.id, "pr:", GithubSchema.TYPE_PULL_REQUEST);
    }

    protected Vertex getComment(final Comment source) throws IllegalAccessException {
        return getVertex(source, source.id, "comment:", GithubSchema.TYPE_COMMENT);
    }

    protected Vertex getRelease(final Release source) throws IllegalAccessException {
        return getVertex(source, source.id, "release:", GithubSchema.TYPE_RELEASE);
    }

    protected Vertex getRepository(final RepositoryBrief source) throws IllegalAccessException {
        return getVertex(source, source.id, "repo:", GithubSchema.TYPE_REPOSITORY);
    }

    protected Vertex getTeam(final Team source) throws IllegalAccessException {
        return getVertex(source, source.id, "team:", GithubSchema.TYPE_TEAM);
    }

    private void setProperties(final Object source,
                               final Vertex target) throws IllegalAccessException {
        Class clazz = source.getClass();
        for (Field f : clazz.getFields()) {
            Object value = f.get(source);
            if (null != value) {
                if (PROPERTY_CLASSES.contains(value.getClass())) {
                    String key = f.getName();
                    key = fixPropertyKey(key);
                    try {
                        target.setProperty(key, value);
                    } catch (IllegalArgumentException e) {
                        // occasionally, Titan will reject certain property values
                        LOGGER.warning("failed to set property " + key + " on vertex " + target.getId() + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    private static final Set<String> RESERVED_KEYS;

    static {
        RESERVED_KEYS = new HashSet<String>();
        for (GithubSchema.Label l : GithubSchema.Label.values()) {
            RESERVED_KEYS.add(l.name());
        }
        RESERVED_KEYS.add("id");
        RESERVED_KEYS.add("label");
    }

    public static String fixPropertyKey(final String key) {
        return RESERVED_KEYS.contains(key) ? key + "_" : key;
    }
}
