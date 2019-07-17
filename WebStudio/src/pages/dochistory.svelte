<script>
  import { onMount } from "svelte";

  export let active = false;
  export let docid = null;
  let jsondata = "";
  let versions = [];
  let page = 1;
  let pages = 1;

  onMount(() => {
    if (docid != null) fillhistory(docid);
  });

  function fillhistory() {
    versions = [];
    jsondata = "";
    window.GET("/raptordb/dochistory?" + docid, function(data) {
      versions = data;
    });
  }

  function showver(ver) {
    window.LOAD("/raptordb/docversion?" + ver, function(data) {
      jsondata = data;
    });
  }
</script>

<style>
  .link {
    color: blue;
    text-decoration: underline;
    cursor: pointer;
    display: block;
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
    color: blue;
    text-decoration: underline;
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
    <label align="top">docid:</label>
    <input
      id="docid"
      autofocus
      style="width:400px"
      on:keypress={fillhistory}
      bind:value={docid} />
    <button
      style="width:50px;"
      on:keypress={event => {
        if ((event.keyCode || event.which) == 13) fillhistory();
      }}>
      Get
    </button>
    <br />
    <br />
    <label align="top">Document Revisions :</label>
    <label style="font-weight: 700">{versions.length}</label>
    {#if versions.length > 0}
      <table style="width:100%">
        <tr>
          <td valign="top" style="width:30%">
            <div class="ListBox">
              {#each versions as v (v)}
                <label class="link" on:click={() => showver(v.Version)}>
                  Version = {v.Version} Date = {v.ChangeDate}
                </label>
              {/each}
            </div>
          </td>
          <td valign="top">
            <div class="JSONArea">
              <div class="JSON">{jsondata}</div>
            </div>
          </td>
        </tr>
      </table>
    {/if}
  </div>
{/if}
