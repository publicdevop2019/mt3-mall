package com.mt.mall.port.adapter.messaging;

import com.mt.common.domain.CommonDomainRegistry;
import com.mt.common.domain.model.restful.exception.AggregateOutdatedException;
import com.mt.common.domain.model.sql.builder.UpdateQueryBuilder;
import com.mt.mall.application.ApplicationServiceRegistry;
import com.mt.mall.domain.model.sku.event.SkuPatchCommandEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static com.mt.mall.domain.model.catalog.event.CatalogEvent.TOPIC_CATALOG;
import static com.mt.mall.domain.model.filter.event.FilterEvent.TOPIC_FILTER;
import static com.mt.mall.domain.model.product.event.ProductCreated.TOPIC_PRODUCT;
import static com.mt.mall.domain.model.tag.event.TagCriticalFieldChanged.TOPIC_TAG;

@Slf4j
@Component
public class DomainEventSubscriber {
    private static final String SKU_QUEUE_NAME = "sku_queue";
    private static final String SKU_EX_QUEUE_NAME = "decrease_sku_for_order_event_mall_handler";
    private static final String SKU_EX_QUEUE_NAME3 = "cancel_decrease_sku_for_order_event_mall_handler";
    private static final String META_QUEUE_NAME = "meta_queue";
    @Value("${spring.application.name}")
    private String appName;
    @Value("${mt.app.name.mt15}")
    private String sagaName;

    @EventListener(ApplicationReadyEvent.class)
    private void skuListener() {
        CommonDomainRegistry.getEventStreamService().subscribe(appName, true, SKU_QUEUE_NAME, (event) -> {
            try {
                ApplicationServiceRegistry.getSkuApplicationService().handleChange(event);
            } catch (UpdateQueryBuilder.PatchCommandExpectNotMatchException | AggregateOutdatedException ex) {
                //ignore above ex
                log.debug("ignore exception in event {}", ex.getClass().toString());
            }
        }, TOPIC_PRODUCT);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME, (event) -> {
            log.debug("handling event with id {}", event.getId());
            SkuPatchCommandEvent deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), SkuPatchCommandEvent.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleDecreaseSkuChange(deserialize);
        }, "decrease_order_storage_event");
    }

    @EventListener(ApplicationReadyEvent.class)
    private void skuExternalListener3() {
        CommonDomainRegistry.getEventStreamService().subscribe(sagaName, false, SKU_EX_QUEUE_NAME3, (event) -> {
            log.debug("handling event with id {}", event.getId());
            SkuPatchCommandEvent deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), SkuPatchCommandEvent.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize);
        }, "cancel_decrease_order_storage_event");
    }

    @EventListener(ApplicationReadyEvent.class)
    private void metaChangeListener() {
        CommonDomainRegistry.getEventStreamService().subscribe(appName, true, META_QUEUE_NAME, (event) -> {
            ApplicationServiceRegistry.getMetaApplicationService().handleChange(event);
        }, TOPIC_TAG, TOPIC_PRODUCT, TOPIC_CATALOG, TOPIC_FILTER);
    }

}
