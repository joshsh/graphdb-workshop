package com.tinkerpop.etc.github.beans;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Payload {

    public String action;
    public Comment comment;
    public Long comment_id;
    public String commit;
    public String description;
    public String head;
    public Long issue;
    public Long issue_id;
    public String master_branch;
    public User member;
    public Integer number;
    public Page[] pages;
    public PullRequest pull_request;
    public String pusher_type;
    public String ref;
    public String ref_type;
    public Release release;
    public Repository repository;
    public Object[] shas;
    public Integer size;
    public Team team;

    public String before;
    public String after;
    public String name;
    public String url;
    public Long id;
    public String desc;

    public User target;
}
