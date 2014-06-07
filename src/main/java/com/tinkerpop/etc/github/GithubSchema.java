package com.tinkerpop.etc.github;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface GithubSchema {

    public enum EventType {
        CommitCommentEvent,
        CreateEvent,
        DeleteEvent,
        ForkApplyEvent,
        ForkEvent,
        GollumEvent,
        IssueCommentEvent,
        IssuesEvent,
        MemberEvent,
        PublicEvent,
        PullRequestEvent,
        PullRequestReviewCommentEvent,
        PushEvent,
        ReleaseEvent,
        TeamAddEvent,
        WatchEvent
    }

    // property names
    public static final String
            CREATED_AT = "created_at",
            EVENT_TYPE = "event_type",
            TYPE = "type";

    // property values
    public static final String
            EVENT = "event",
            REPOSITORY = "repository",
            USER = "user";

    // edge labels
    public static final String
            PUSHED_BY = "pushedBy",
            PUSHED_TO = "pushedTo";
}
