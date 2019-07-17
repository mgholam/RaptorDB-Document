<script>
  export let active = false;

  let find = "";
  let page = 1;
  let versions = [];
  let jsondata = "";
  let count = 20;
  let total = 0;
  let start = 0;
  $: pages = Math.ceil(total / count);

  function docsearch() {
    start = (page - 1) * count;
    window.GET(
      "/raptordb/docsearch?" + find + "&start=" + start + "&count=" + count,
      function(res) {
        versions = res.Items;
        total = res.TotalCount;
      }
    );
  }

  function next() {
    if (page < pages) page = page + 1;
    docsearch();
  }

  function prev() {
    if (page > 1) page = page - 1;
    docsearch();
  }

  function showver(ver) {
    window.LOAD("/raptordb/docversion?" + ver, function(res) {
      jsondata = res;
    });
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
    <label align="top">Search:</label>
    <input
      id="search"
      autofocus
      style="width:400px"
      on:keypress={event => {
        if ((event.keyCode || event.which) == 13) docsearch();
      }}
      bind:value={find} />
    <button style="width:50px;" on:click={docsearch}>Get</button>
    <br />
    <br />
    <label align="top">Documents found :</label>
    <label style="font-weight: 700">{total}</label>
    <label style="color:red" id="err" />
      {#if total > 0}
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
                {#each versions as v (v)}
                  <label on:click={() => showver(v)}>
                    Document number = {v}
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
