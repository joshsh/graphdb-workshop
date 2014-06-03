package com.tinkerpop.etc.github;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.etc.github.beans.Event;
import com.tinkerpop.etc.github.beans.RepositoryBrief;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GithubLoader {
    private static final Logger LOGGER = Logger.getLogger(GithubLoader.class.getName());

    private static final int
            DEFAULT_BUFFER_SIZE = 1000,
            DEFAULT_LOGGING_BUFFER_SIZE = 10000;

    // e.g. 2014-05-31T00:13:30-07:00
    public static final DateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private int loggingBufferSize = DEFAULT_LOGGING_BUFFER_SIZE;
    private boolean verbose = false;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Graph graph;

    private long countToCommit = 0;

    public GithubLoader(final Graph graph) {
        objectMapper.configure(MapperFeature.USE_ANNOTATIONS, true);
        this.graph = graph;
    }

    /**
     * @param bufferSize the number of statements to be added per transaction commit
     */
    public void setBufferSize(final int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * @param verbose whether to log information on individual files loaded and statements added
     */
    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * @param loggingBufferSize the number of events per logging message (if verbose=true)
     */
    public void setLoggingBufferSize(final int loggingBufferSize) {
        this.loggingBufferSize = loggingBufferSize;
    }

    /**
     * Loads a GitHub Archive event dump file, or a directory containing event dumps
     *
     * @param fileOrDirectory a line-delimited JSON file or a directory which contains (at any level)
     *                        one or more line-delimited JSON files
     *                        Files must be named with an appropriate extension, i.e. .json or .ldjson,
     *                        or an appropriate extension followed by .gz if they are compressed with Gzip.
     */
    public synchronized void load(final File fileOrDirectory) throws Exception {
        LOGGER.info("loading from " + fileOrDirectory);
        // TODO: open graph
        try {
            long startTime = System.currentTimeMillis();

            long count = loadRecursive(fileOrDirectory);

            // commit leftover statements
            commit();

            long endTime = System.currentTimeMillis();

            LOGGER.info("loaded " + count + " events in " + (endTime - startTime) + "ms");
        } finally {
            rollback();
        }
    }

    private long loadRecursive(final File fileOrDirectory) throws Exception {
        if (fileOrDirectory.isDirectory()) {
            long count = 0;

            for (File child : fileOrDirectory.listFiles()) {
                count += loadRecursive(child);
            }

            return count;
        } else {
            long startTime = System.currentTimeMillis();
            if (verbose) {
                LOGGER.info("loading file: " + fileOrDirectory);
            }
            String fileName = fileOrDirectory.getName();
            InputStream is;
            if (fileName.endsWith(".gz")) {
                is = new GZIPInputStream(new FileInputStream(fileOrDirectory));
                fileName = fileName.substring(0, fileName.lastIndexOf("."));
            } else {
                is = new FileInputStream(fileOrDirectory);
            }

            try {
                if (!fileName.endsWith(".json") && !fileName.endsWith(".ldjson")) {
                    LOGGER.warning("file does not appear to be line-delimited JSON: " + fileName);
                    return 0;
                }

                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;
                long lineNo = 0;
                while (null != (line = br.readLine())) {
                    lineNo++;

                    try {
                        parseGithubJson(line.trim());
                    } catch (InvalidEventException e) {
                        LOGGER.warning("invalid event on line " + lineNo + " in " + fileName);
                        throw e;
                    } catch (Exception e) {
                        LOGGER.severe("error on line " + lineNo + " in " + fileName);
                        throw e;
                    }
                }

                long endTime = System.currentTimeMillis();
                if (verbose) {
                    LOGGER.info("\tfinished reading " + lineNo + " lines in " + (endTime - startTime) + "ms");
                }

                return lineNo;
            } finally {
                is.close();
            }
        }
    }

    private void parseGithubJson(final String jsonStr) throws IOException, InvalidEventException {
        Event event = objectMapper.readValue(jsonStr, Event.class);
        //System.out.println("got event: " + event);

        // TODO: add to the graph

        GithubSchema.EventType eventType = GithubSchema.EventType.valueOf(event.type);

        switch (eventType) {
            case CommitCommentEvent:
                break;
            case CreateEvent:
                break;
            case DeleteEvent:
                break;
            case ForkEvent:
                break;
            case GollumEvent:
                break;
            case IssueCommentEvent:
                break;
            case IssuesEvent:
                break;
            case MemberEvent:
                break;
            case PublicEvent:
                break;
            case PullRequestEvent:
                break;
            case PullRequestReviewCommentEvent:
                break;
            case PushEvent:
                handlePushEvent(event, eventType);
                break;
            case ReleaseEvent:
                break;
            case TeamAddEvent:
                break;
            case WatchEvent:
                break;
        }

        commitIncremental();
    }

    private void commitIncremental() {
        countToCommit++;
        if (0 == countToCommit % bufferSize) {
            commit();

            if (verbose && 0 == countToCommit % loggingBufferSize) {
                LOGGER.info("" + System.currentTimeMillis() + "\t" + countToCommit);
            }
        }
    }

    private void commit() {
        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        }
    }

    private void rollback() {
        if (graph instanceof TransactionalGraph) {
            ((TransactionalGraph) graph).commit();
        }
    }

    private Vertex getOrCreateVertex(final String id,
                                     final String type) {
        Vertex v = null == id ? null : graph.getVertex(id);
        if (null == v) {
            v = graph.addVertex(id);
            v.setProperty(GithubSchema.TYPE, type);
        }

        return v;
    }

    private String repoId(final RepositoryBrief repo) {
        //return "repo:" + repo.id;
        return "repo:" + repo.owner + ":" + repo.name;
    }

    private String actorId(final String actor) {
        return "user:" + actor;
    }

    private void handlePushEvent(final Event event,
                                 final GithubSchema.EventType eventType) throws InvalidEventException {
        if (null == event.repository) {
            LOGGER.warning("null repository in " + GithubSchema.EventType.PushEvent);
            return;
        }

        long timestamp = parseTimestamp(event.created_at);

        RepositoryBrief repo = event.repository;
        Vertex repoVertex = getOrCreateVertex(repoId(repo), GithubSchema.REPOSITORY);

        Vertex userVertex = getOrCreateVertex(actorId(event.actor), GithubSchema.USER);

        Vertex eventVertex = getOrCreateVertex(null, GithubSchema.EVENT);
        eventVertex.setProperty(GithubSchema.EVENT_TYPE, eventType.name());

        Edge e1 = graph.addEdge(null, eventVertex, userVertex, GithubSchema.PUSHED_BY);
        e1.setProperty(GithubSchema.CREATED_AT, timestamp);

        Edge e2 = graph.addEdge(null, eventVertex, repoVertex, GithubSchema.PUSHED_TO);
        e2.setProperty(GithubSchema.CREATED_AT, timestamp);
    }

    private long parseTimestamp(final String timestamp) throws InvalidEventException {
        Date d;

        try {
            d = TIMESTAMP_FORMAT.parse(timestamp);
        } catch (ParseException e) {
            throw new InvalidEventException("not a valid timestamp: " + timestamp);
        }

        return d.getTime();
    }

    private class InvalidEventException extends Exception {
        public InvalidEventException(final String message) {
            super(message);
        }
    }

    public static void main(final String[] args) throws Exception {
        Graph g = new TinkerGraph();

        GithubLoader loader = new GithubLoader(g);
        //loader.setVerbose(true);

        loader.load(new File("/Users/josh/tmp/github"));
        //loader.load(new File("/Users/josh/projects/fortytwo/laboratory/test/texaslinuxfest/examples/2014-05-30-0_1000.json"));
    }
}
