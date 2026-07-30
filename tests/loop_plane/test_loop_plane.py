import pytest
from hfa.loop_plane import *
from hfa.loop_plane.errors import *
from hfa.loop_plane.reducer import rebuild


def contract():
    return LoopContract('c', '1', 'p1', ('unit',), 2, 1)


def runtime_event(kind='TaskAdmitted'):
    return {'event_id':'r1','event_type':kind,'run_id':'run','task_id':'task','canonical_operation_id':'op','source_transition_id':'tx','source_task_revision':1,'occurred_at_ms':1,'correlation_id':'corr','causation_id':'cause','loop_id':'loop'}


def test_off_zero_write():
    store = InMemoryPrototypeLoopStore()
    service = LoopPlaneService(store, LoopMode.OFF)
    with pytest.raises(ModeViolation): service.observe(runtime_event(), 0)
    assert store.load_events('loop') == []


def test_observe_translated():
    store = InMemoryPrototypeLoopStore()
    result = LoopPlaneService(store, LoopMode.OBSERVE).observe(runtime_event(), 0)
    assert result.status is TranslationStatus.TRANSLATED
    assert len(store.load_events('loop')) == 1


def test_unsupported_fails_closed():
    store = InMemoryPrototypeLoopStore()
    result = LoopPlaneService(store, LoopMode.OBSERVE).observe(runtime_event('Bogus'), 0)
    assert result.status is TranslationStatus.UNSUPPORTED_EVENT_TYPE
    assert not store.load_events('loop')


def test_mode_boundaries():
    service = LoopPlaneService(InMemoryPrototypeLoopStore(), LoopMode.OBSERVE)
    with pytest.raises(ModeViolation): service.evaluate()
    with pytest.raises(ModeViolation): service.proposal()


def test_deep_immutable():
    payload = {'x': {'y': 1}}
    event = LoopEvent('e', 'l', 1, 'LoopStarted', 1, 'c', 'r', payload)
    payload['x']['y'] = 2
    assert event.payload['x']['y'] == 1
    with pytest.raises(TypeError): event.payload['x']['y'] = 3


def test_receipt_binding():
    store = InMemoryPrototypeLoopStore()
    event = LoopEvent('e','l',1,'LoopStarted',1,'c','r',{'contract':contract(),'run_id':'r','task_id':'t'})
    receipt = CommandReceipt('START','l','k','0'*64,('wrong',),1,'c','r')
    with pytest.raises(ReceiptConflict): store.commit('l',[event],receipt,0)
    assert store.load_events('l') == []


def test_idempotency():
    store = InMemoryPrototypeLoopStore()
    event = LoopEvent('e','l',1,'LoopStarted',1,'c','r',{'contract':contract(),'run_id':'r','task_id':'t'})
    receipt = CommandReceipt('START','l','k','0'*64,('e',),1,'c','r')
    assert store.commit('l',[event],receipt,0) == receipt
    assert store.commit('l',[event],receipt,0) == receipt
    conflict = CommandReceipt('START','l','k','1'*64,('e',),1,'c','r')
    with pytest.raises(IdempotencyConflict): store.commit('l',[event],conflict,1)


def test_unknown_event_rejected():
    from hfa.loop_plane.reducer import apply
    from hfa.loop_plane.state import LoopState
    with pytest.raises(InvalidEventStream): apply(LoopState('l'), LoopEvent('e','l',1,'X',1,'c','r',{}))


def test_contract_required_criterion():
    results, _ = verify(contract(), [], loop_id='l', attempt_id='a', task_id='t', run_id='r', input_hash='i', now_ms=1, generator_principal='g')
    assert results['unit'] is Outcome.UNKNOWN
    assert recommend(contract(), results)[0] is Recommendation.BLOCK


def test_proposal_safety():
    with pytest.raises(ValueError):
        CanonicalCommandProposal('p','l','a','d',Recommendation.RETRY_RECOMMENDED,'TASK_REQUEUE',1,'x','0'*64,('t',),'p',1,False,True,True,'APPROVED')


def test_rebuild_deterministic():
    events = [
        LoopEvent('1','l',1,'LoopStarted',1,'c','r',{'contract':contract(),'run_id':'r','task_id':'t'}),
        LoopEvent('2','l',2,'AttemptObserved',2,'c','r',{'attempt_id':'a'}),
    ]
    assert rebuild('l', events) == rebuild('l', list(events))


def test_closed_rejects_attempt():
    from hfa.loop_plane.reducer import apply
    state = rebuild('l', [
        LoopEvent('1','l',1,'LoopStarted',1,'c','r',{'contract':contract(),'run_id':'r','task_id':'t'}),
        LoopEvent('2','l',2,'AttemptObserved',2,'c','r',{'attempt_id':'a'}),
        LoopEvent('3','l',3,'ShadowDecisionRecorded',3,'c','r',{'attempt_id':'a','recommendation':'ACCEPT'}),
    ])
    with pytest.raises(DomainTransitionError): apply(state, LoopEvent('4','l',4,'AttemptObserved',4,'c','r',{'attempt_id':'b'}))
