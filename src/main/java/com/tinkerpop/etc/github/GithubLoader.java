package com.tinkerpop.etc.github;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.etc.github.beans.Event;
import com.tinkerpop.etc.github.beans.RepositoryBrief;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GithubLoader {
    private static final Logger LOGGER = Logger.getLogger(GithubLoader.class.getName());

    public static final DateFormat
            OLD_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z"), // e.g. 2012/03/11 00:00:00 -0800
            NEW_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"); // e.g. 2014-05-31T00:13:30-07:00

    private static final String
            DOWNLOAD_DIRECTORY = "downloadDirectory",
            LAST_FILE_LOADED = "lastFileLoaded",
            START_HOUR = "startHour",
            END_HOUR = "endHour";

    private boolean verbose = false;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Graph graph;
    private final File statusFile;
    private final Properties configuration;
    private File lastFileLoaded;
    private String downloadDirectory;
    private GithubTimestamp startHour, endHour;

    private long countToCommit = 0;

    private final Comparator<File> fileComparator = new GitHubArchiveFileComparator();

    public GithubLoader(final File statusFile) throws IOException {
        objectMapper.configure(MapperFeature.USE_ANNOTATIONS, true);
        this.statusFile = statusFile;

        configuration = new Properties();

        InputStream in = new FileInputStream(statusFile);
        try {
            configuration.load(in);
        } finally {
            in.close();
        }

        String s = configuration.getProperty(LAST_FILE_LOADED);
        if (null != s) {
            lastFileLoaded = new File(s);
        }

        downloadDirectory = configuration.getProperty(DOWNLOAD_DIRECTORY);

        s = configuration.getProperty(START_HOUR);
        if (null == s) {
            s = "2012-03-10-22";

            // earliest archive files are in a slightly different format
            //s = "2011-02-12-00";
        }
        startHour = new GithubTimestamp(s);

        s = configuration.getProperty(END_HOUR);
        if (null != s) {
            endHour = new GithubTimestamp(s);
        }

        String storageBackend = configuration.getProperty("storage.backend");
        String keyspace = configuration.getProperty("storage.keyspace", "github");
        if (null == storageBackend) {
            LOGGER.warning("no storage backend specified.  Using TinkerGraph");
            graph = GraphFactory.createTinkerGraph();
        } else if (storageBackend.equals("cassandra")) {
            String host = configuration.getProperty("storage.hostname", "127.0.0.1");
            graph = GraphFactory.createTitanOnCassandra(host, keyspace);
        } else if (storageBackend.equals("berkeleyje")) {
            String dir = configuration.getProperty("storage.directory", "/tmp/github");
            graph = GraphFactory.createTitanOnBerkeleyJE(dir, keyspace);
        } else {
            throw new IllegalStateException("unsupported storage backend: " + storageBackend);
        }
    }

    private void saveConfiguration() throws IOException {
        configuration.setProperty(LAST_FILE_LOADED, lastFileLoaded.getAbsolutePath());

        OutputStream out = new FileOutputStream(statusFile);
        try {
            configuration.store(out, "GitHub Archive loader state");
        } finally {
            out.close();
        }
    }

    /**
     * @param verbose whether to log information on individual files loaded and statements added
     */
    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Loads a GitHub Archive event dump file, or a directory containing event dumps
     */
    public synchronized void loadFiles() throws Exception {
        if (null == downloadDirectory) {
            throw new IllegalStateException("" + DOWNLOAD_DIRECTORY + " must be set");
        }

        LOGGER.info("loading from " + downloadDirectory);

        try {
            long startTime = System.currentTimeMillis();

            long count = loadRecursive(new File(downloadDirectory));

            // commit leftover statements
            commit();

            long endTime = System.currentTimeMillis();

            LOGGER.info("loaded " + count + " events in " + (endTime - startTime) + "ms");
        } finally {
            rollback();
        }
    }

    /**
     * @param fileOrDirectory a line-delimited JSON file or a directory which contains (at any level)
     *                        one or more line-delimited JSON files
     *                        Files must be named with an appropriate extension, i.e. .json or .ldjson,
     *                        or an appropriate extension followed by .gz if they are compressed with Gzip.
     */
    private long loadRecursive(final File fileOrDirectory) throws Exception {
        if (fileOrDirectory.isDirectory()) {
            long count = 0;

            List<File> files = sortedFiles(fileOrDirectory);

            for (File child : files) {
                count += loadRecursive(child);
            }

            return count;
        } else if (null == lastFileLoaded || fileComparator.compare(lastFileLoaded, fileOrDirectory) < 0) {
            try {
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

                    lastFileLoaded = fileOrDirectory;
                    saveConfiguration();

                    commit();

                    return lineNo;
                } finally {
                    is.close();
                }
            } finally {
                rollback();
            }
        } else {
            return 0;
        }
    }

    private List<File> sortedFiles(final File dir) {
        List<File> files = new LinkedList<File>();
        Collections.addAll(files, dir.listFiles());
        Collections.sort(files, fileComparator);
        return files;
    }

    private void parseGithubJson(final String jsonStr) throws IOException, InvalidEventException {
        Event event = objectMapper.readValue(jsonStr, Event.class);
        //System.out.println("got event: " + event);

        GithubSchema.EventType eventType = GithubSchema.EventType.valueOf(event.type);

        switch (eventType) {
            case CommitCommentEvent:
                break;
            case CreateEvent:
                break;
            case DeleteEvent:
                break;
            case FollowEvent:
                break;
            case ForkEvent:
                break;
            case GistEvent:
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
            d = NEW_TIMESTAMP_FORMAT.parse(timestamp);
        } catch (ParseException e) {
            try {
                d = OLD_TIMESTAMP_FORMAT.parse(timestamp);
            } catch (ParseException e1) {
                throw new InvalidEventException("not a valid timestamp: " + timestamp);
            }
        }

        return d.getTime();
    }

    private class InvalidEventException extends Exception {
        public InvalidEventException(final String message) {
            super(message);
        }
    }

    private void downloadFiles() throws IOException {
        if (null == downloadDirectory) {
            throw new IllegalStateException("" + DOWNLOAD_DIRECTORY + " must be set");
        }
        // wget http://data.githubarchive.org/2014-05-31-{0..23}.json.gz

        File dir = new File(downloadDirectory);

        for (File f : dir.listFiles()) {
            if (f.getName().endsWith(".tmp")) {
                f.delete();
            }
        }

        List<File> files = sortedFiles(dir);
        if (files.size() > 0) {
            GithubTimestamp lastTimestamp = new GithubTimestamp(files.get(files.size() - 1));

            if (lastTimestamp.compareTo(startHour) >= 0) {
                startHour = lastTimestamp.nextHour();
            }
        }

        GithubTimestamp cur = startHour;
        while (true) {
            long now = System.currentTimeMillis();
            long curTime = cur.getTime();
            if (curTime > now || (null != endHour && cur.compareTo(endHour) > 0)) {
                break;
            }

            URL url = new URL("http://data.githubarchive.org/" + cur + ".json.gz");
            File f = new File(downloadDirectory, "" + cur + ".json.gz.tmp");

            LOGGER.info("downloading " + url);
            ReadableByteChannel rbc;
            try {
                rbc = Channels.newChannel(url.openStream());
            } catch (FileNotFoundException e) {
                LOGGER.info("no such file: " + url + " ");

                // just skip; there are missing hours in the archive
                continue;
            } finally {
                cur = cur.nextHour();
            }

            FileOutputStream fos = new FileOutputStream(f);
            try {
                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            } finally {
                fos.close();
            }

            File f2 = new File(downloadDirectory, "" + cur + ".json.gz");
            f.renameTo(f2);
        }
    }

    private class GitHubArchiveFileComparator implements Comparator<File> {
        public int compare(final File f1, final File f2) {
            return new GithubTimestamp(f1).compareTo(new GithubTimestamp(f2));
        }
    }

    public static void main(final String[] args) throws Exception {
        File config = args.length > 0 ? new File(args[0]) : new File("githubloader.props");
        if (!config.exists()) {
            exitWithError("configuration file does not exist: " + config.getAbsoluteFile());
        }

        GithubLoader loader = new GithubLoader(config);
        loader.setVerbose(true);

        //loader.downloadFiles();

        loader.loadFiles();
    }

    private static void exitWithError(final String message) {
        System.err.println(message);
        System.exit(1);
    }
}
