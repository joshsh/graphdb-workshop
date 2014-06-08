package com.tinkerpop.etc.github.beans;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class PullRequest {

    public int additions;
    public User assignee;
    public Revision base;
    public String body;
    public int changed_files;
    public String closed_at;
    public int comments;
    public String comments_url;
    public int commits;
    public String commits_url;
    public String created_at;
    public int deletions;
    public String diff_url;
    public Revision head;
    public String html_url;
    public long id;
    public String issue_url;
    public Links links;
    public String mergeable;
    public String mergeable_state;
    public boolean merged;
    public String merged_at;
    public User merged_by;
    public String merge_commit_sha;
    public Milestone milestone;
    public int number;
    public String patch_url;
    public int review_comments;
    public String review_comments_url;
    public String review_comment_url;
    public String state;
    public String statuses_url;
    public String title;
    public String updated_at;
    public String url;
    public User user;
    public Links _links;

    public String status_url;
}
