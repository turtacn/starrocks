package com.starrocks.partial.failure;

import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class FailureDetectorTest {

    private FailureDetector failureDetector;
    private TabletFailureRepository mockRepo;
    private SystemInfoService mockSystemInfo;
    private TabletInvertedIndex mockInvertedIndex;
    private GlobalStateMgr mockGlobalStateMgr;

    @Before
    public void setUp() {
        mockRepo = mock(TabletFailureRepository.class);
        mockSystemInfo = mock(SystemInfoService.class);
        mockInvertedIndex = mock(TabletInvertedIndex.class);
        mockGlobalStateMgr = mock(GlobalStateMgr.class);

        GlobalStateMgr.setDummytGSM(mockGlobalStateMgr);
        when(mockGlobalStateMgr.getTabletInvertedIndex()).thenReturn(mockInvertedIndex);
        when(mockGlobalStateMgr.getNodeMgr().getClusterInfo()).thenReturn(mockSystemInfo);

        failureDetector = new FailureDetector(mockRepo);
    }

    @Test
    public void testRecordFailedTablets() {
        long backendId = 1L;
        when(mockInvertedIndex.getTabletIdsByBackendId(backendId)).thenReturn(Arrays.asList(100L, 101L));

        failureDetector.recordFailedTablets(backendId);

        verify(mockRepo, times(2)).save(any(TabletFailure.class));
    }

    @Test
    public void testHandleNodeRecovery() {
        long backendId = 1L;
        TabletFailure failure = new TabletFailure(100L, backendId, 200L, 300L, System.currentTimeMillis(), com.starrocks.partial.TabletStatus.FAILED);
        when(mockInvertedIndex.getTabletIdsByBackendId(backendId)).thenReturn(Arrays.asList(100L));
        when(mockRepo.findByTabletId(100L)).thenReturn(failure);

        failureDetector.handleNodeRecovery(backendId);

        verify(mockRepo, times(1)).save(any(TabletFailure.class));
        assert(failure.getStatus() == com.starrocks.partial.TabletStatus.RECOVERING);
    }
}
