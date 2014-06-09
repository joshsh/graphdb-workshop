package com.tinkerpop.etc.github.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Repository {

    public String archive_url;
    public String assignees_url;
    public String blobs_url;
    public String branches_url;
    public String clone_url;
    public String collaborators_url;
    public String comments_url;
    public String commits_url;
    public String compare_url;
    public String contents_url;
    public String contributors_url;
    public String created_at;
    public String default_branch;
    public String description;
    public String downloads_url;
    public String events_url;
    public Boolean fork;
    public Integer forks;
    public Integer forks_count;
    public String forks_url;
    public String full_name;
    public String git_commits_url;
    public String git_refs_url;
    public String git_tags_url;
    public String git_url;
    public Boolean has_downloads;
    public Boolean has_issues;
    public Boolean has_wiki;
    public String homepage;
    public String hooks_url;
    public String html_url;
    public Long id;
    public String issues_url;
    public String issue_comment_url;
    public String issue_events_url;
    public String keys_url;
    public String labels_url;
    public String language;
    public String languages_url;
    public String merges_url;
    public String milestones_url;
    public String mirror_url;
    public String name;
    public String notifications_url;
    public Integer open_issues;
    public Integer open_issues_count;
    public User owner;
    public Permissions permissions;

    @JsonProperty("private")
    public Boolean private_;

    public String pulls_url;
    public String pushed_at;
    public String releases_url;
    public Integer size;
    public String ssh_url;
    public Integer stargazers_count;
    public String stargazers_url;
    public String statuses_url;
    public String subscribers_url;
    public String subscription_url;
    public String svn_url;
    public String tags_url;
    public String teams_url;
    public String trees_url;
    public String updated_at;
    public String url;
    public Integer watchers;
    public Integer watchers_count;

    public String master_branch;
    public Urls urls;
    public Links _links;
}
