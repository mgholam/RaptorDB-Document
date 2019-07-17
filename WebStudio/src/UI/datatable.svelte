<script>
  import { createEventDispatcher, tick } from "svelte";

  export let rows = [];
  export let totalrows = 1;
  export let page = 1;

  const dispatch = createEventDispatcher();
  let rowspp = 10;
  let sortcol = "";
  let sorttype = "";
  $: totalpages = Math.ceil(totalrows / rowspp); 

  function nextpage() {
    if (page < totalpages) page = page + 1;
    refresh();
  }

  function prevpage() {
    if (page > 1) page = page - 1;
    refresh();
  }

  function refresh() {
    var d = {
      start: (page - 1) * rowspp,
      count: rowspp,
      sort: sortcol + " " + sorttype,
      page
    };
    dispatch("refresh", d);
  }

  function sortby(col) {
    if (sortcol !== col) {
      sortcol = col;
      sorttype = "ASC";
    } else sorttype = sorttype === "ASC" ? "DESC" : "ASC";
    refresh();
  }

  function showdoc(docid) {
    // show doc
    dispatch("showdoc", docid);
  }

  function page10() {
    rowspp = 10;
    page = 1;
    refresh();
  }
  function page25() {
    rowspp = 25;
    page = 1;
    refresh();
  }
  function page50() {
    rowspp = 50;
    page = 1;
    refresh();
  }
  function page100() {
    rowspp = 100;
    page = 1;
    refresh();
  }
</script>

<style>
  .link {
    color: blue;
    text-decoration: underline;
    cursor: pointer;
  }

  .cssTbl {
    margin: 0px;
    padding: 3px;
    font-family: Arial;
    font-weight: normal;
  }

  .cssTbl table {
    border-spacing: 0;
    width: 100%;
    margin: 0px;
    padding: 0px;
  }

  .cssTbl th {
    background-color: #333333;
    padding: 5px;
    cursor: pointer;
    font-weight: normal;
    color: white;
  }

  .cssTbl th:hover {
    color: red;
  }

  .cssTbl tr:nth-child(odd) {
    background-color: #c0c0c0;
  }

  .cssTbl tr:nth-child(even) {
    background-color: #ffffff;
  }

  .cssTbl td {
    vertical-align: middle;
    border: 1px solid #666666;
    text-align: left;
    padding: 5px;
    color: #000000;
  }
</style>

{#if rows !== null && rows.length > 0}
  <pre>Rows : {rows.length} of {totalrows.toLocaleString()}</pre>
  <div class="pager">
    <label class="link" on:click={prevpage}>&lt;prev</label>
    <label>{page} of {totalpages.toLocaleString()}</label>
    <label class="link" on:click={nextpage}>next&gt;</label>
    <label style="padding-left:100px">Rows per Page :</label>
    <label class="link" style="padding-left:10px" on:click={page10}>10</label>
    <label class="link" style="padding-left:10px" on:click={page25}>25</label>
    <label class="link" style="padding-left:10px" on:click={page50}>50</label>
    <label class="link" style="padding-left:10px" on:click={page100}>100</label>
  </div>

  <div class="cssTbl">
    <table>
      <tr>
        {#each Object.keys(rows[0]) as col (col)}
          <th on:click={() => sortby(col)}>{col}</th>
        {/each}
      </tr>
      {#each rows as row}
        <tr>
          {#each Object.keys(row) as r (r)}
            <td>
              {#if r === 'docid'}
                <label class="link" on:click={() => showdoc(row[r])}>
                  {row[r]}
                </label>
              {:else}{row[r]}{/if}
            </td>
          {/each}
        </tr>
      {/each}
    </table>
  </div>

  <div class="pager">
    <label class="link" on:click={prevpage}>&lt;prev</label>
    <label>{page} of {totalpages.toLocaleString()}</label>
    <label class="link" on:click={nextpage}>next&gt;</label>
  </div>
{:else}
  <p>No data</p>
{/if}
