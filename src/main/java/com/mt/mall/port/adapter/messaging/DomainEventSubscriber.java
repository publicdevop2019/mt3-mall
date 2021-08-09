package com.mt.mall.port.adapter.messaging;

import com.mt.common.domain.CommonDomainRegistry;
import com.mt.common.domain.model.domain_event.MQHelper;
import com.mt.common.domain.model.restful.exception.AggregateOutdatedException;
import com.mt.common.domain.model.sql.builder.UpdateQueryBuilder;
import com.mt.mall.application.ApplicationServiceRegistry;
import com.mt.mall.application.sku.command.InternalSkuPatchCommand;
import com.mt.mall.infrastructure.AppConstant;
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
    private static final String SKU_QUEUE_NAME = "sku_queue"+AppConstant.APP_HANDLER;
    private static final String META_QUEUE_NAME = "meta_queue"+AppConstant.APP_HANDLER;
    @Value("${spring.application.name}")
    private String appName;
    @Value("${mt.app.name.mt15}")
    private String sagaName;

    @EventListener(ApplicationReadyEvent.class)
    private void listener() {
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
    private void listener1() {
        CommonDomainRegistry.getEventStreamService().subscribe(appName, true, META_QUEUE_NAME, (event) -> {
            ApplicationServiceRegistry.getMetaApplicationService().handleChange(event);
        }, TOPIC_TAG, TOPIC_PRODUCT, TOPIC_CATALOG, TOPIC_FILTER);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void listener2() {
        CommonDomainRegistry.getEventStreamService().of(sagaName, false,AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT,(event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    MQHelper.replyOf(AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT));
        });
    }

    @EventListener(ApplicationReadyEvent.class)
    private void listener3() {
        CommonDomainRegistry.getEventStreamService().cancelOf(sagaName, false,  AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    MQHelper.replyCancelOf(AppConstant.DECREASE_ORDER_STORAGE_FOR_CREATE_EVENT));
        });
    }

    @EventListener(ApplicationReadyEvent.class)
    private void listener4() {
        CommonDomainRegistry.getEventStreamService().of(sagaName, false, AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    MQHelper.replyOf(AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT));
        });
    }
    @EventListener(ApplicationReadyEvent.class)
    private void listener5() {
        CommonDomainRegistry.getEventStreamService().of(sagaName, false, AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    MQHelper.replyOf(AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT));
        });
    }
    @EventListener(ApplicationReadyEvent.class)
    private void listener6() {
        CommonDomainRegistry.getEventStreamService().of(sagaName, false, AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handle(deserialize,
                    MQHelper.replyOf(AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT));
        });
    }
    @EventListener(ApplicationReadyEvent.class)
    private void listener7() {
        CommonDomainRegistry.getEventStreamService().cancelOf(sagaName, false, AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    MQHelper.replyCancelOf(AppConstant.DECREASE_ACTUAL_STORAGE_FOR_CONCLUDE_EVENT));
        });
    }
    @EventListener(ApplicationReadyEvent.class)
    private void listener8() {
        CommonDomainRegistry.getEventStreamService().cancelOf(sagaName, false, AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    MQHelper.replyCancelOf(AppConstant.INCREASE_ORDER_STORAGE_FOR_RECYCLE_EVENT));
        });
    }
    @EventListener(ApplicationReadyEvent.class)
    private void listener9() {
        CommonDomainRegistry.getEventStreamService().cancelOf(sagaName, false, AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT, (event) -> {
            InternalSkuPatchCommand deserialize = CommonDomainRegistry.getCustomObjectSerializer().deserialize(event.getEventBody(), InternalSkuPatchCommand.class);
            ApplicationServiceRegistry.getSkuApplicationService().handleCancel(deserialize,
                    MQHelper.replyCancelOf(AppConstant.DECREASE_ORDER_STORAGE_FOR_RESERVE_EVENT));
        } );
    }
}
