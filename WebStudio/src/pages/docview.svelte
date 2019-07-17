<script>
  import { onMount, createEventDispatcher } from "svelte";
  export let active = false;
  export let docid = null;
  let doc = "";
  const dispatch = createEventDispatcher();

  function docview() {
    doc = "";
    window.LOAD("/raptordb/docget?" + docid, function(data) {
      doc = data;
    });
  }

  function showrevs() {
    dispatch("showrevs", docid);
  }

  onMount(() => {
    if (docid != null) docview();
  });
</script>

<style>
  .link {
    color: blue;
    text-decoration: underline;
    cursor: pointer;
    margin-left: 20px;
  }
</style>

<svelte:options accessors={true} />

{#if active}
  <div class="tab-content" class:active>
    <label align="top">docid:</label>
    <input
      id="docid"
      autofocus
      style="width:400px"
      on:keypress={event => {
        if ((event.keyCode || event.which) == 13) docview();
      }}
      bind:value={docid} />
    <button style="width:50px;" on:click={docview}>Get</button>
    <br />
    <br />
    {#if doc !== ''}
      <div class="disp">
        <label align="top">Document Revisions :</label>
        <label style="font-weight: 700" />
        <label class="link" style="margin-left:5px" on:click={showrevs}>
          See Revisions
        </label>
        <br />
        <label>Document JSON :</label>
        <div class="JSONArea">
          <div class="JSON" id="data">{doc}</div>
        </div>
      </div>
    {/if}
  </div>
{/if}
