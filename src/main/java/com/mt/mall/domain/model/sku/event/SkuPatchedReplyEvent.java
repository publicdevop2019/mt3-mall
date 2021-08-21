package com.mt.mall.domain.model.sku.event;

import com.mt.common.domain.model.domain_event.DomainEvent;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SkuPatchedReplyEvent extends DomainEvent {
    private boolean emptyOpt;
    private long taskId;

    public SkuPatchedReplyEvent(boolean emptyOpt, long taskId,String topic) {
        setEmptyOpt(emptyOpt);
        setTaskId(taskId);
        setInternal(false);
        setTopic(topic);
    }
}
