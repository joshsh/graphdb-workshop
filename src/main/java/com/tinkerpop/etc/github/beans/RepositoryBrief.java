package com.tinkerpop.etc.github.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class RepositoryBrief {

    public String created_at;
    public String description;
    public boolean fork;
    public int forks;
    public boolean has_downloads;
    public boolean has_issues;
    public boolean has_wiki;
    public String homepage;
    public long id;
    public String language;
    public String master_branch;
    public String name;
    public int open_issues;
    public String organization;
    public String owner;

    @JsonProperty("private")
    public boolean private_;

    public String pushed_at;
    public int size;
    public int stargazers;
    public String url;
    public int watchers;
    public String integrate_branch;
}
