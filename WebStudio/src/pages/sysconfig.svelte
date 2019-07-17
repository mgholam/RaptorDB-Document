<script>
  import { onMount } from "svelte";
  export let active = false;

  let configs = "";

  // config
  onMount(() => {
    window.LOAD("/raptordb/action?getconfigs", function(data) {
      configs = data;
    });
  });
</script>

<style>

</style>

<svelte:options accessors={true} />

{#if active}
  <div class="tab-content" class:active>
    <h2>raptordb.config</h2>
    <div class="JSONArea">
      <pre class="JSON">{configs}</pre>
    </div>
  </div>
{/if}
