const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('static/device-location.js','utf8');

function harness(geoEnabled=true) {
  let success, failure, options, calls=0;
  const cleared=[], positions=[], states=[];
  const geo={watchPosition(ok,err,opts){success=ok;failure=err;options=opts;calls++;return 0;},clearWatch(id){cleared.push(id);}};
  const sandbox={window:{},navigator:{geolocation:geoEnabled?geo:undefined},Date,Number,Math};
  vm.runInNewContext(source,sandbox);
  const tracker=sandbox.window.FuelLocation.createTracker({onPosition:p=>positions.push(p),onState:s=>states.push(s)});
  return {tracker,positions,states,cleared,success:p=>success(p),failure:e=>failure(e),get options(){return options;},get calls(){return calls;}};
}
const fix=(latitude,longitude,accuracy=20)=>({coords:{latitude,longitude,accuracy},timestamp:Date.now()});

test('tracks successive actual device fixes and prevents duplicate watches',()=>{
  const h=harness();h.tracker.start();h.tracker.start();
  h.success(fix(-27.4,153));h.success(fix(-27.41,153.01));
  assert.equal(h.calls,1);assert.equal(h.positions.length,2);
  assert.equal(h.positions[1].latitude,-27.41);assert.equal(h.positions[1].accuracy,20);
  assert.equal(h.options.enableHighAccuracy,true);assert.equal(h.states.at(-1).active,true);
});
test('stop clears watch ID zero and rejects late callbacks',()=>{
  const h=harness();h.tracker.start();h.success(fix(-27.4,153));h.tracker.stop();h.success(fix(-20,140));
  assert.deepEqual(h.cleared,[0]);assert.equal(h.positions.at(-1),null);assert.equal(h.positions.length,2);
  assert.equal(h.states.at(-1).active,false);
});
test('denied permission clears coordinates and explains recovery',()=>{
  const h=harness();h.tracker.start();h.failure({code:1});
  assert.equal(h.states.at(-1).active,false);assert.match(h.states.at(-1).message,/permission was denied/);
  assert.equal(h.positions.at(-1),null);assert.deepEqual(h.cleared,[0]);
});
test('unavailable platform never pretends to have a location',()=>{
  const h=harness(false);h.tracker.start();assert.equal(h.calls,0);assert.equal(h.positions.length,0);
  assert.match(h.states.at(-1).message,/unavailable/);
});
test('initial timeout ends the pending watch and offers a clean retry',()=>{
  const h=harness();h.tracker.start();h.failure({code:3});
  assert.equal(h.states.at(-1).active,false);assert.deepEqual(h.cleared,[0]);
  assert.match(h.states.at(-1).message,/retry/);h.tracker.start();assert.equal(h.calls,2);
});
test('rejects invalid coordinates and labels stale fixes',()=>{
  const h=harness();h.tracker.start();h.success(fix(100,153));assert.equal(h.positions.length,0);
  h.success({...fix(-27.4,153),timestamp:Date.now()-180000});assert.equal(h.positions.at(-1).stale,true);
});
