package com.tinkerpop.etc.github.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class RepositoryBrief {

    public String created_at;
    public String description;
    public Boolean fork;
    public Integer forks;
    public Boolean has_downloads;
    public Boolean has_issues;
    public Boolean has_wiki;
    public String homepage;
    public Long id;
    public String language;
    public String master_branch;
    public String name;
    public Integer open_issues;
    public String organization;
    public String owner;

    @JsonProperty("private")
    public Boolean private_;

    public String pushed_at;
    public Integer size;
    public Integer stargazers;
    public String url;
    public Integer watchers;

    public String integrate_branch;
}
