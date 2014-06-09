package com.tinkerpop.etc.github;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface GithubSchema {

    public enum EventType {
        CommitCommentEvent,
        CreateEvent,
        DeleteEvent,
        DownloadEvent,
        FollowEvent,
        ForkApplyEvent,
        ForkEvent,
        GistEvent,
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
            TIMESTAMP = "timestamp",
            TYPE = "type_";

    // property values
    public static final String
            TYPE_COMMENT = "Comment",
            TYPE_EVENT = "Event",
            TYPE_PAGE = "Page",
            TYPE_PULL_REQUEST = "PullRequest",
            TYPE_RELEASE = "Release",
            TYPE_REPOSITORY = "Repository",
            TYPE_TEAM = "Team",
            TYPE_USER = "User";

    public enum Label {
        actor,
        comment,
        member,
        page,
        pull_request,
        release,
        repository,
        target,
        team
    }
}
