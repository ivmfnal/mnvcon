xxx_iovs (
    iov_id      bigserial   primary key,
    begin_time  timestamp,
    active      boolean
);

xxx_tags (
    tag         text    primary key,
    created     timestamp,
    comments    text
);

xxx_tag_iovs (
    tag     text    references xxx_tags(tag) on delete cascade,
    iov_id  bigint  references xxx_iovs(iov_id) on delete cascade,
    primary key (tag, iov_id)
);

xxx_data (
    __iov_id    bigint  references xxx_iovs(iov_id) on delete cascade,
    channel     bigint,
    ...
    primary key(__iov_id, channel)
);    
