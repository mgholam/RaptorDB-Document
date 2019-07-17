<script>
  import { onMount } from "svelte";

  export let active = false;
  let DataFolderSize = 0;
  let DocumentCount = 0;
  let FileCount = 0;
  let HighFrequncyItems = 0;
  let MemoryUsage = 0;
  let NumberOfViews = 0;
  let OSVersion = 0;
  let RaptorDBVersion = 0;
  let Uptime = 0;
  let logs = [];

  onMount(() => {
    refresh();
  });

  function refresh() {
    window.GET("/raptordb/systeminfo", function(data) {
      logs = data.LogItems;
      DataFolderSize = data["DataFolderSize"];
      DocumentCount = data["DocumentCount"];
      FileCount = data["FileCount"];
      HighFrequncyItems = data["HighFrequncyItems"];
      MemoryUsage = data["MemoryUsage"];
      NumberOfViews = data["NumberOfViews"];
      OSVersion = data["OSVersion"];
      RaptorDBVersion = data["RaptorDBVersion"];
      Uptime = data["Uptime"];
    });
  }
  function backup() {
    // TODO : backup
  }
  function compact() {
    // TODO : compact
  }
</script>

<style>
  .bold {
    font-weight: 700;
  }

  button {
    margin-right: 20px;
  }
</style>

<svelte:options accessors={true} />

{#if active}
  <div class="tab-content" class:active>
    <button on:click={refresh}>Refresh</button>
    <!-- <button on:click={backup}>Backup data</button>
    <button on:click={compact}>Compact HF key store</button> -->
    <br />
    <br />

    <div class="data">
      <label>Data Folder Size :</label>
      <label class="bold">{DataFolderSize.toLocaleString() +" bytes"}</label>
      <br />
      <label>Document Object Count :</label>
      <label class="bold">{DocumentCount.toLocaleString()}</label>
      <br />
      <label>File Object Count :</label>
      <label class="bold">{FileCount.toLocaleString()}</label>
      <br />
      <label>High Frequncy Items :</label>
      <label class="bold">{HighFrequncyItems.toLocaleString()}</label>
      <br />
      <label>Memory Usage :</label>
      <label class="bold">{MemoryUsage}</label>
      <br />
      <label>Number of Views :</label>
      <label class="bold">{NumberOfViews}</label>
      <br />
      <label>OS Version :</label>
      <label class="bold">{OSVersion}</label>
      <br />
      <label>RaptorDB Version :</label>
      <label class="bold">{RaptorDBVersion}</label>
      <br />
      <label>Uptime :</label>
      <label class="bold">{Uptime}</label>
      <br />
      <label>Last Logs:</label>
      <div class="LogArea">
        {#each logs as l (l)}
          <div class="LogItems">{l}</div>
        {/each}
      </div>
    </div>
  </div>
{/if}
