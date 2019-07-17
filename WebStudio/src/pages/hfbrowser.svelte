<script>
  import { onMount, createEventDispatcher } from "svelte";
  export let active = false;
  const dispatch = createEventDispatcher();

  let key = "";
  let keys = [];
  let json = "";
  let start = 0;
  let count = 10;
  let total = 0;
  let page = 1;

  $: pages = Math.ceil(total / count);

  onMount(() => {
    getall();
  });

  function getall() {
    start = (page - 1) * count;
    window.GET(
      "/raptordb/hfkeys?" + key + "&start=" + start + "&count=" + count,
      function(data) {
        keys = data.Items;
        total = data.TotalCount;
      }
    );
  }

  function showkey(k) {
    window.LOAD("/raptordb/hfget?" + k, function(res) {
      json = res;
    });
  }

  function next() {
    if (page < pages) page = page + 1;
    getall();
  }

  function prev() {
    if (page > 1) page = page - 1;
    getall();
  }
</script>

<style>
  .link {
    color: blue;
    text-decoration: underline;
    cursor: pointer;
  }
  .ListBox {
    border-style: solid;
    border-color: gray;
    background-color: white;
    padding: 5px;
    height: 500px;
    overflow: scroll;
  }

  .ListBox label {
    display: block;
    padding: 2px;
  }

  .ListBox label:hover {
    background-color: #c0c0c0;
  }
</style>

<svelte:options accessors={true} />

{#if active}
  <div class="tab-content" class:active>
    <button on:click={getall}>Get All Keys</button>
    <br />
    <br />
    <label align="top">Get Key :</label>
    <input
      id="docid"
      autofocus
      style="width:400px"
      on:keypress={event => {
        if ((event.keyCode || event.which) == 13) showkey(key);
      }}
      bind:value={key} />
    <button style="width:50px;" on:click={() => showkey(key)}>Get</button>
    <br />
    <br />
    <label align="top">HF Keys Count :</label>
    <label style="font-weight: 700">{total}</label>
    <label style="color:red" id="err" />
    <div>
      <table style="width:100%">
        <tr>
          <td style="width:30%">
            <div>
              <label class="link" on:click={prev}>&lt;prev</label>
              <label>{page} of {pages}</label>
              <label class="link" on:click={next}>next&gt;</label>
            </div>
          </td>
          <td>
            <div>
              <label style="color:red;" id="err" />
            </div>
          </td>
        </tr>
        <tr>
          <td valign="top">
            <div class="ListBox">
              {#each keys as v (v)}
                <label class="link" on:click={() => showkey(v)}>
                  Key = {v}
                </label>
              {/each}
            </div>
          </td>
          <td valign="top">
            <div class="JSONArea">
              <div class="JSON" id="data">{json}</div>
            </div>
          </td>
        </tr>
      </table>
    </div>
  </div>
{/if}
