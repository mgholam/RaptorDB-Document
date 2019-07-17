<script>
  import { onMount, tick } from "svelte";
  import DataTable from "../UI/datatable.svelte";

  export let active = false;
  let viewname = "";

  let viewnames = [];
  let colnames = [];

  let data = null;
  let start = 0;
  let count = 10;
  let filter = "";
  let sort = "";
  let totalrows = 1;
  let page = 1;
  let errormsg = "";
  let qtime = "";

  onMount(() => {
    // get views from server
    errormsg = "";
    window.GET("RaptorDB/GetViews", function(data) {
      // console.log(data);
      var vn = [];
      data.Rows.forEach(x => vn.push(x.Name));
      viewnames = vn;
      if (vn.length > 0) {
        viewname = vn[0];
        schemachanged();
      }
    });
  });

  function schemachanged() {
    page = 1;
    start = 0;
    // console.log(viewname);
    window.GET("RaptorDB/GetSchema?view=" + viewname, function(data) {
      var vn = [];
      data.Rows.forEach(x => vn.push(x.ColumnName));
      // console.log(vn);
      colnames = vn;
    });
  }

  function run() {
    errormsg = "";
    var url =
      "raptordb/views/" +
      viewname +
      (filter !== "" ? "?" + filter : "") +
      "?start=" +
      start +
      "?count=" +
      count +
      (sort !== " " ? "?orderby=" + sort : "");

    var startTime = new Date().getTime();
    window.GET(
      url,
      function(ret) {
        totalrows = ret.TotalCount;
        data = ret.Rows;
        qtime = new Date().getTime() - startTime + "ms";
      },
      function(err) {
        errormsg = err;
        data = [];
      }
    );
  }

  function excel() {
    var s =
      window.ServerURL + "RaptorDB/ExcelExport/" + viewname + "?" + filter;
    window.open(s);
  }

  function refresh(event) {
    // get start count
    start = event.detail.start;
    count = event.detail.count;
    sort = event.detail.sort;
    run();
  }

  function addcolumn(col) {
    filter = filter + col;
  }
</script>

<style>
  .link {
    color: blue;
    text-decoration: underline;
    cursor: pointer;
    display: block;
  }
  .link2 {
    color: blue;
    text-decoration: underline;
    cursor: pointer;
    margin-left: 20px;
  }
  .ListBox {
    border-style: solid;
    border-color: gray;
    background-color: white;
    padding: 5px;
    height: 500px;
    overflow: scroll;
  }
</style>

<svelte:options accessors={true} />

{#if active}
  <div class="tab-content" class:active>
    <label>View Name :</label>
    <select style="width:200px; font-size: larger" on:change={schemachanged} bind:value={viewname}>
      {#each viewnames as vn (vn)}
        <option>{vn}</option>
      {/each}
    </select>
    <br />
    <br />

    <label align="top">Filter:</label>
    <table>
      <tr>
        <td>
          <div
            id="columns"
            style="height:150px;width:150px;display:inline-block;"
            class="ListBox">
            {#each colnames as col (col)}
              <label class="link" on:click={() => addcolumn(' ' + col)}>
                {col}
              </label>
            {/each}
          </div>
        </td>
        <td>
          <label class="link2" on:click={() => addcolumn(' AND')}>AND</label>
          <label class="link2" on:click={() => addcolumn(' OR')}>OR</label>
          <label class="link2" on:click={() => addcolumn('.between(,)')}>
            Between
          </label>
          <label class="link2" on:click={() => addcolumn('.year')}>Year</label>
          <label class="link2" on:click={() => addcolumn('.month')}>
            Month
          </label>
          <label class="link2" on:click={() => addcolumn('.day')}>Day</label>
          <label
            class="link2"
            style="float:right;"
            on:click={() => (filter = '')}>
            Clear
          </label>
          <br />
          <textarea
            id="filter"
            cols="80"
            style="height:156px;resize:none;border-color:grey;border-width:medium;"
            bind:value={filter} />
        </td>
      </tr>
    </table>
    <button
      style="width:200px;height:40px"
      on:click={() => {
        page = 1;
        start = 0;
        run();
      }}>
      Run
    </button>
    <button style="width:200px;height:40px" on:click={excel}>
      Export to Excel
    </button>
    <br />

    {#if errormsg === ''}
      {#if data != null}
        <pre>Query time (+render) : {qtime}</pre>
      {/if}
      <DataTable
        on:refresh={event => refresh(event)}
        on:showdoc
        bind:rows={data}
        bind:totalrows
        bind:page />
    {:else}
      <p style="color:red">{errormsg}</p>
    {/if}

  </div>
{/if}
