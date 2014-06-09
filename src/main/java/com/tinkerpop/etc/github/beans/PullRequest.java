package com.tinkerpop.etc.github.beans;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class PullRequest {

    public Integer additions;
    public User assignee;
    public Revision base;
    public String body;
    public Integer changed_files;
    public String closed_at;
    public Integer comments;
    public String comments_url;
    public Integer commits;
    public String commits_url;
    public String created_at;
    public Integer deletions;
    public String diff_url;
    public Revision head;
    public String html_url;
    public Long id;
    public String issue_url;
    public Links links;
    public String mergeable;
    public String mergeable_state;
    public Boolean merged;
    public String merged_at;
    public User merged_by;
    public String merge_commit_sha;
    public Milestone milestone;
    public Integer number;
    public String patch_url;
    public Integer review_comments;
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
